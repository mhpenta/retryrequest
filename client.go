// Package retryrequest is an HTTP client that retries transient failures
// within bounds you set: an attempt limit, an elapsed-time budget, a cap on
// each wait, and the caller's context deadline.
//
// It retries transport errors, 408, 425, 429, and most 5xx responses with
// jittered exponential backoff, honoring Retry-After. Requests that are not
// idempotent, such as POST, are sent once unless they carry an idempotency
// key or the client opts in. GetBytes and PostBytes read the body inside each
// attempt, so a truncated body is fetched again; GetReader and Do return the
// open response. Failures come back as *StatusError, *RetryError, or the
// underlying error, all usable with errors.Is and errors.As.
package retryrequest

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v7"
)

const maxDrainBytes = 64 << 10

// defaultHTTPClient has no overall Timeout, which would also cut off long
// response bodies. ResponseHeaderTimeout catches a server that never answers;
// the caller's context and WithPerAttemptTimeout bound the rest.
var defaultHTTPClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: time.Second,
	},
}

// Client makes HTTP requests with retries. Construct it with New. A Client can
// be shared by concurrent callers.
type Client struct {
	httpClient         *http.Client
	newBackOff         func() backoff.BackOff
	maxAttempts        uint
	maxElapsed         time.Duration
	maxWait            time.Duration
	maxWaitSet         bool
	retryIf            func(*http.Response, error) bool
	headers            http.Header
	maxBodyBytes       int64
	perAttemptTimeout  time.Duration
	logger             *slog.Logger
	fallbackUserAgent  bool
	utf8Conversion     bool
	retryNonIdempotent bool
	idempotencyHeader  string
	beforeAttempt      func(*http.Request, int) error
}

// New constructs a client. Defaults are four attempts, a 30-second retry
// window, jittered exponential backoff with a 10-second maximum wait, and a
// 50 MiB limit on helper output.
func New(opts ...Option) *Client {
	c := &Client{
		httpClient:   defaultHTTPClient,
		maxAttempts:  4,
		maxElapsed:   30 * time.Second,
		maxWait:      10 * time.Second,
		retryIf:      DefaultRetryIf,
		headers:      make(http.Header),
		maxBodyBytes: 50 << 20,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.newBackOff == nil {
		c.newBackOff = c.defaultBackOff
		c.maxWaitSet = true // the default backoff is always capped
	}
	return c
}

// Do sends req using ctx in place of req's context. It returns open 2xx
// responses. The caller closes the body, which releases the per-attempt
// timeout. Failed responses are drained and closed here. Do leaves decoding
// and read limits to the caller or the injected transport.
//
// Like http.Client.Do, Do takes ownership of req.Body. Retries of a request
// with a body require req.GetBody to return a fresh copy. Requests with a
// method that is not idempotent, such as POST or PATCH, are sent once unless
// they carry an idempotency key or the client was built with
// WithRetryNonIdempotent.
func (c *Client) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	resp, _, err := c.do(ctx, req, nil)
	return resp, err
}

// readFunc consumes a 2xx response within an attempt and always closes its body.
type readFunc func(*http.Response) ([]byte, error)

type attemptResult struct {
	resp    *http.Response
	payload []byte
}

// do runs the retry loop. With a nil read it returns the open response. With a
// read it consumes the body inside each attempt, so a body cut off mid-transfer
// is retried under the same attempt and time budget as a failed status.
func (c *Client) do(ctx context.Context, req *http.Request, read readFunc) (*http.Response, []byte, error) {
	host := req.URL.Hostname()
	schedule := &retrySchedule{
		ctx:     ctx,
		BackOff: c.newBackOff(),
		maxWait: c.maxWait,
		capped:  c.maxWaitSet,
	}
	hasBody := req.Body != nil && req.Body != http.NoBody
	request := c.requestWithDefaults(ctx, req)
	retryMethod := c.prepareReplay(request, req.Header)
	replayable := retryMethod && (!hasBody || req.GetBody != nil)

	var (
		attempts int
		lastErr  error
		fallback bool
	)

	// finish ends a failed attempt: it stops on cancellation or a final
	// failure, and otherwise asks backoff to schedule another attempt.
	finish := func(retry bool) (attemptResult, error) {
		if ctx.Err() != nil {
			return attemptResult{}, backoff.Permanent(contextError(ctx, lastErr))
		}
		if !retry {
			return attemptResult{}, backoff.Permanent(lastErr)
		}
		// Keep the real cause outside Retry so a transport error wrapping
		// backoff.RetryAfter cannot override the HTTP wait policy.
		return attemptResult{}, errRetry
	}

	operation := func() (attemptResult, error) {
		if ctx.Err() != nil {
			if attempts == 0 && hasBody {
				_ = req.Body.Close()
			}
			return attemptResult{}, backoff.Permanent(contextError(ctx, lastErr))
		}
		attempt := request.Clone(ctx)
		if fallback {
			attempt.Header.Del("User-Agent")
		}
		if attempts > 0 && hasBody {
			body, err := req.GetBody()
			if err != nil {
				return attemptResult{}, backoff.Permanent(errors.Join(lastErr, fmt.Errorf("replay request body: %w", err)))
			}
			attempt.Body = body
		}
		attempts++
		schedule.retryAfter = -1
		if c.beforeAttempt != nil {
			if err := c.beforeAttempt(attempt, attempts); err != nil {
				if attempt.Body != nil {
					_ = attempt.Body.Close()
				}
				return attemptResult{}, backoff.Permanent(errors.Join(lastErr, fmt.Errorf("before attempt %d: %w", attempts, err)))
			}
		}

		resp, err := c.send(attempt)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if read == nil {
				return attemptResult{resp: resp}, nil
			}
			payload, readErr := read(resp)
			if readErr == nil {
				return attemptResult{payload: payload}, nil
			}
			lastErr = readErr
			return finish(ctx.Err() == nil && replayable && !permanentReadError(readErr) && c.retryIf(resp, readErr))
		}

		lastErr = err
		if err == nil {
			// Read what draining would read anyway, so the connection can be
			// reused, and keep the bytes for the error and the retry.
			raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxDrainBytes))
			_ = resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewReader(raw))
			status := responseError(resp, host, raw)
			lastErr = status
			if status.StatusCode == http.StatusTooManyRequests || status.StatusCode == http.StatusServiceUnavailable {
				schedule.retryAfter = status.RetryAfter
			}
		}
		// On a redirect error net/http has already closed any response body.
		retry := ctx.Err() == nil && !terminalError(err) && replayable && c.retryIf(resp, err)
		fallback = fallback || (retry && c.fallbackUserAgent && err != nil && resp == nil)
		return finish(retry)
	}

	result, err := backoff.Retry(ctx, operation,
		backoff.WithBackOff(schedule),
		backoff.WithMaxTries(c.maxAttempts),
		backoff.WithMaxElapsedTime(c.maxElapsed),
		backoff.WithNotify(func(_ error, wait time.Duration) {
			if c.logger != nil {
				c.logger.InfoContext(ctx, "Retrying HTTP request", "host", host, "attempt", attempts, "wait", wait)
			}
		}),
	)
	if err == nil {
		return result.resp, result.payload, nil
	}
	outcome := backoff.AsRetryError(err)
	if outcome == nil {
		// backoff documents a *RetryError for every failure; keep any other
		// error rather than dereferencing nil.
		return nil, nil, err
	}
	if !errors.Is(outcome.LastErr, errRetry) {
		return nil, nil, outcome.LastErr
	}
	reason := schedule.stopReason
	if reason == "" && ctx.Err() != nil {
		return nil, nil, contextError(ctx, lastErr)
	}
	if errors.Is(outcome.Cause, backoff.ErrMaxElapsedTime) {
		reason = "maximum retry elapsed time exceeded"
	}
	return nil, nil, &RetryError{Attempts: attempts, Reason: reason, Err: lastErr}
}

// prepareReplay resolves the key once on the request cloned for each attempt.
// Client-wide default headers do not opt a request into replay or stand in for
// a generated key, since their values are shared across calls.
func (c *Client) prepareReplay(req *http.Request, callerHeaders http.Header) bool {
	switch req.Method {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace, http.MethodPut, http.MethodDelete:
		return true
	}
	hasKey := false
	for _, header := range []string{"Idempotency-Key", "X-Idempotency-Key", c.idempotencyHeader} {
		if header == "" {
			continue
		}
		if strings.TrimSpace(callerHeaders.Get(header)) != "" {
			hasKey = true
		}
		if strings.TrimSpace(req.Header.Get(header)) == "" {
			// net/http treats even an empty standard key as a replay signal.
			req.Header.Del(header)
		}
	}
	if !hasKey && c.idempotencyHeader != "" {
		req.Header.Set(c.idempotencyHeader, newIdempotencyKey())
		hasKey = true
	}
	return hasKey || c.retryNonIdempotent
}

func newIdempotencyKey() string {
	// Generate a version 4 UUID.
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (c *Client) requestWithDefaults(ctx context.Context, req *http.Request) *http.Request {
	request := req.Clone(ctx)
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	for key, values := range c.headers {
		if _, exists := request.Header[key]; !exists {
			request.Header[key] = values
		}
	}
	return request
}

func (c *Client) send(req *http.Request) (*http.Response, error) {
	if c.perAttemptTimeout <= 0 {
		return c.httpClient.Do(req)
	}
	ctx, cancel := context.WithTimeout(req.Context(), c.perAttemptTimeout)
	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		cancel()
		return resp, err
	}
	body := resp.Body
	resp.Body = &responseReader{Reader: body, close: func() error {
		defer cancel()
		return body.Close()
	}}
	return resp, nil
}

func contextError(ctx context.Context, lastErr error) error {
	cause := context.Cause(ctx)
	if !errors.Is(cause, ctx.Err()) {
		cause = errors.Join(ctx.Err(), cause)
	}
	if lastErr == nil || errors.Is(cause, lastErr) {
		return cause
	}
	return errors.Join(cause, lastErr)
}
