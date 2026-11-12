package retryrequest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cenkalti/backoff/v7"
)

func fixedBackOff(delay time.Duration) Option {
	return WithBackOff(func() backoff.BackOff { return backoff.NewConstantBackOff(delay) })
}

func TestRetryStatuses(t *testing.T) {
	for _, tt := range []struct {
		retry bool
		codes []int
	}{
		{true, []int{408, 425, 429, 500, 502, 503, 504, 522, 599}},
		{false, []int{302, 400, 401, 403, 404, 409, 410, 422, 451, 501, 505, 511}},
	} {
		for _, code := range tt.codes {
			t.Run(fmt.Sprint(code), func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					if calls.Add(1) == 1 {
						w.WriteHeader(code)
						return
					}
					_, _ = io.WriteString(w, "ok")
				}))
				defer server.Close()

				got, err := New(fixedBackOff(0)).GetBytes(t.Context(), server.URL)
				if tt.retry {
					if err != nil || string(got) != "ok" || calls.Load() != 2 {
						t.Fatalf("body = %q, error = %v, attempts = %d", got, err, calls.Load())
					}
				} else {
					var status *StatusError
					if !errors.As(err, &status) || status.StatusCode != code || status.Host != "127.0.0.1" || calls.Load() != 1 {
						t.Fatalf("error = %#v, attempts = %d", err, calls.Load())
					}
				}
			})
		}
	}
}

func TestCustomRetryPolicy(t *testing.T) {
	var codes []int
	client := New(fixedBackOff(0),
		WithRetryIf(func(resp *http.Response, err error) bool {
			if err != nil {
				t.Fatalf("status predicate received transport error %v", err)
			}
			codes = append(codes, resp.StatusCode)
			return resp.StatusCode == http.StatusNotFound
		}),
		WithHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			code := http.StatusNotFound
			if len(codes) == 1 {
				code = http.StatusServiceUnavailable
			}
			return &http.Response{StatusCode: code, Body: http.NoBody}, nil
		})}))
	_, err := client.GetBytes(t.Context(), "https://example.test")
	var status *StatusError
	if !errors.As(err, &status) || status.StatusCode != 503 || len(codes) != 2 {
		t.Fatalf("error = %v, predicate saw statuses %v", err, codes)
	}
}

func TestRetryAfter(t *testing.T) {
	for _, tt := range []struct {
		name   string
		code   int
		header func(time.Time) (string, time.Time)
		cap    time.Duration
		stop   bool
	}{
		{"seconds floor", 429, func(now time.Time) (string, time.Time) {
			return "1", now.Add(time.Second)
		}, time.Second, false},
		{"date floor", 503, func(now time.Time) (string, time.Time) {
			at := now.Add(2 * time.Second).UTC().Truncate(time.Second)
			return at.Format(http.TimeFormat), at
		}, 2 * time.Second, false},
		{"seconds cutoff", 503, func(now time.Time) (string, time.Time) {
			return "3600", now.Add(time.Hour)
		}, time.Second, true},
		{"date cutoff", 429, func(now time.Time) (string, time.Time) {
			at := now.Add(time.Hour).UTC().Truncate(time.Second)
			return at.Format(http.TimeFormat), at
		}, time.Second, true},
		{"ignored on 500", 500, func(now time.Time) (string, time.Time) {
			return "3600", now
		}, time.Second, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			floors := make(chan time.Time, 1)
			retries := make(chan time.Time, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				now := time.Now()
				if calls.Add(1) == 1 {
					header, floor := tt.header(now)
					floors <- floor
					w.Header().Set("Retry-After", header)
					w.WriteHeader(tt.code)
					return
				}
				retries <- now
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err := New(fixedBackOff(0), WithMaxRetryWait(tt.cap)).GetBytes(ctx, server.URL)
			if tt.stop {
				var status *StatusError
				var exhausted *RetryError
				if !errors.As(err, &status) || status.StatusCode != tt.code || status.RetryAfter <= tt.cap ||
					!errors.As(err, &exhausted) || !strings.Contains(exhausted.Reason, "Retry-After") || calls.Load() != 1 {
					t.Fatalf("error = %v, attempts = %d", err, calls.Load())
				}
				return
			}
			if err != nil || calls.Load() != 2 {
				t.Fatalf("error = %v, attempts = %d", err, calls.Load())
			}
			if at, floor := <-retries, <-floors; at.Before(floor) {
				t.Fatalf("retried %s before the Retry-After floor", floor.Sub(at))
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	for value, want := range map[string]time.Duration{
		"": -1, "later": -1, "-1": -1, "1.5": -1, "99999999999999999999": -1,
		"0": 0, " 00120 ": 2 * time.Minute,
		"Thu, 12 Nov 2026 12:00:30 GMT": 30 * time.Second,
		"Thu, 12 Nov 2026 11:00:00 GMT": 0,
	} {
		if got := parseRetryAfter(value, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", value, got, want)
		}
	}
}

func TestWaitBudgets(t *testing.T) {
	for _, tt := range []struct {
		name    string
		options []Option
		timeout time.Duration
		want    time.Duration
		reason  string
	}{
		{"context deadline", nil, time.Second, 0, "context deadline"},
		{"elapsed budget", []Option{WithMaxElapsedTime(time.Second)}, 0, 0, "elapsed"},
		{"max wait cap", []Option{WithMaxRetryWait(time.Second)}, 0, time.Second, ""},
		{"retry after below backoff", nil, 0, 2 * time.Second, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
					calls++
					if calls == 1 {
						return &http.Response{StatusCode: 503, Header: http.Header{"Retry-After": {"1"}}, Body: http.NoBody}, nil
					}
					return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
				})
				opts := append([]Option{fixedBackOff(2 * time.Second), WithHTTPClient(&http.Client{Transport: transport})}, tt.options...)
				ctx := t.Context()
				if tt.timeout != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tt.timeout)
					defer cancel()
				}
				start := time.Now()
				_, err := New(opts...).GetBytes(ctx, "https://example.test")
				if elapsed := time.Since(start); elapsed != tt.want {
					t.Fatalf("elapsed = %s, want %s", elapsed, tt.want)
				}
				if tt.reason == "" {
					if err != nil || calls != 2 {
						t.Fatalf("error = %v, attempts = %d", err, calls)
					}
					return
				}
				var exhausted *RetryError
				var status *StatusError
				if !errors.As(err, &exhausted) || !strings.Contains(exhausted.Reason, tt.reason) ||
					!errors.As(err, &status) || status.StatusCode != 503 || ctx.Err() != nil ||
					errors.Is(err, context.DeadlineExceeded) || calls != 1 {
					t.Fatalf("error = %v, attempts = %d, context = %v", err, calls, ctx.Err())
				}
			})
		})
	}
}

func TestPostReplay(t *testing.T) {
	for _, replayable := range []bool{true, false} {
		t.Run(fmt.Sprint(replayable), func(t *testing.T) {
			const payload = "some request data"
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				body, err := io.ReadAll(req.Body)
				if err != nil || string(body) != payload || req.Method != http.MethodPost ||
					req.Header.Get("Content-Type") != "application/test" {
					t.Errorf("method = %s, content type = %s, body = %q, error = %v", req.Method, req.Header.Get("Content-Type"), body, err)
				}
				if calls.Add(1) == 1 {
					w.WriteHeader(http.StatusTooManyRequests)
					return
				}
				_, _ = io.WriteString(w, "ok")
			}))
			defer server.Close()

			client := New(fixedBackOff(0), WithRetryNonIdempotent())
			if replayable {
				got, err := client.PostBytes(t.Context(), server.URL, "application/test", []byte(payload))
				if err != nil || string(got) != "ok" || calls.Load() != 2 {
					t.Fatalf("body = %q, error = %v, attempts = %d", got, err, calls.Load())
				}
				return
			}
			req, err := http.NewRequest(http.MethodPost, server.URL, io.NopCloser(strings.NewReader(payload)))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/test")
			resp, err := client.Do(t.Context(), req)
			if resp != nil {
				resp.Body.Close()
			}
			var status *StatusError
			if resp != nil || !errors.As(err, &status) || status.StatusCode != 429 || calls.Load() != 1 {
				t.Fatalf("response = %v, error = %v, attempts = %d", resp, err, calls.Load())
			}
		})
	}
}

func TestGetBodyFailurePreservesFirstFailure(t *testing.T) {
	cause := errors.New("request data is no longer available")
	calls := 0
	client := New(fixedBackOff(0), WithRetryNonIdempotent(), WithHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		req.Body.Close()
		return &http.Response{StatusCode: http.StatusBadGateway, Body: http.NoBody}, nil
	})}))
	req, err := http.NewRequest(http.MethodPost, "https://example.test", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	req.GetBody = func() (io.ReadCloser, error) { return nil, cause }
	_, err = client.Do(t.Context(), req)
	var status *StatusError
	if !errors.Is(err, cause) || !errors.As(err, &status) || status.StatusCode != 502 || calls != 1 {
		t.Fatalf("error = %v, attempts = %d", err, calls)
	}
}

func TestAttemptsCountedOnce(t *testing.T) {
	for _, transportFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(transportFailure), func(t *testing.T) {
			cause := errors.New("transport failure")
			calls := 0
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				if transportFailure {
					return nil, cause
				}
				return &http.Response{StatusCode: http.StatusBadGateway, Body: http.NoBody}, nil
			})}
			_, err := New(WithHTTPClient(client), WithMaxAttempts(3), fixedBackOff(0)).
				GetBytes(t.Context(), "http://example.test")
			var exhausted *RetryError
			if !errors.As(err, &exhausted) || exhausted.Attempts != 3 || calls != 3 {
				t.Fatalf("error = %v, attempts = %d", err, calls)
			}
			var status *StatusError
			if transportFailure && !errors.Is(err, cause) || !transportFailure && !errors.As(err, &status) {
				t.Fatalf("last cause missing from %v", err)
			}
		})
	}
}

func TestCancellationDuringWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		reason := errors.New("stopped by caller")
		time.AfterFunc(time.Second, func() { cancel(reason) })
		calls := 0
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: 503, Body: http.NoBody}, nil
		})}
		_, err := New(WithHTTPClient(client), fixedBackOff(5*time.Second)).GetBytes(ctx, "https://example.test")
		var status *StatusError
		if !errors.Is(err, context.Canceled) || !errors.Is(err, reason) || !errors.As(err, &status) || calls != 1 {
			t.Fatalf("error = %v, attempts = %d", err, calls)
		}
	})
}

func TestTerminalTransportErrors(t *testing.T) {
	for _, cause := range []error{
		context.Canceled,
		&BodyTooLargeError{Limit: 1024},
		backoff.Permanent(errors.New("rejected by transport")),
	} {
		t.Run(cause.Error(), func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return nil, cause
			})}
			_, err := New(WithHTTPClient(client), WithRetryIf(func(*http.Response, error) bool {
				t.Fatal("terminal error reached the retry predicate")
				return true
			})).GetBytes(t.Context(), "https://example.test")
			if !errors.Is(err, cause) || calls != 1 {
				t.Fatalf("error = %v, attempts = %d", err, calls)
			}
		})
	}
}

func TestFallbackUserAgentAndHeaderIsolation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if agent := req.UserAgent(); !strings.HasPrefix(agent, "Go-http-client/") {
			t.Errorf("fallback User-Agent = %q", agent)
		}
		if got := req.Header.Values("X-Default"); strings.Join(got, ",") != "first,second" {
			t.Errorf("default header = %v", got)
		}
		if got := req.Header.Get("X-Override"); got != "request" {
			t.Errorf("overridden header = %q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	base := server.Client().Transport
	var initial, fallback atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.UserAgent() != "" {
			initial.Add(1)
			if req.UserAgent() != "from-request" {
				t.Errorf("initial User-Agent = %q", req.UserAgent())
			}
			return nil, errors.New("custom user agent rejected")
		}
		fallback.Add(1)
		return base.RoundTrip(req)
	})
	client := New(fixedBackOff(0), WithUserAgentFallback(), WithUserAgent("configured"),
		WithHeader("X-Default", "first"), WithHeader("X-Default", "second"), WithHeader("X-Override", "default"),
		WithHTTPClient(&http.Client{Transport: transport}))
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("User-Agent", "from-request")
	req.Header.Set("X-Override", "request")
	const workers = 24
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			resp, err := client.Do(t.Context(), req)
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
		})
	}
	wg.Wait()
	if initial.Load() != workers || fallback.Load() != workers || req.UserAgent() != "from-request" || req.Header.Get("X-Default") != "" {
		t.Fatalf("initial = %d, fallback = %d, original headers = %v", initial.Load(), fallback.Load(), req.Header)
	}
}

func TestFailedDrainAndTimeoutOwnership(t *testing.T) {
	failedBody := &countedBody{Reader: strings.NewReader(strings.Repeat("x", 2*maxDrainBytes))}
	calls := 0
	var attemptCtx context.Context
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			attemptCtx = req.Context()
			return &http.Response{StatusCode: 500, Body: failedBody}, nil
		}
		if attemptCtx.Err() == nil {
			t.Error("failed attempt context is still live")
		}
		attemptCtx = req.Context()
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("success"))}, nil
	})
	client := New(WithHTTPClient(&http.Client{Transport: transport}), fixedBackOff(0), WithPerAttemptTimeout(time.Minute))
	req, err := http.NewRequest(http.MethodGet, "https://example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil || string(got) != "success" || attemptCtx.Err() != nil {
		t.Errorf("body = %q, read error = %v, context error = %v", got, err, attemptCtx.Err())
	}
	if err := resp.Body.Close(); err != nil || attemptCtx.Err() != context.Canceled {
		t.Errorf("close error = %v, context error = %v", err, attemptCtx.Err())
	}
	if !failedBody.closed || failedBody.read != maxDrainBytes {
		t.Errorf("failed response closed = %t, read = %d", failedBody.closed, failedBody.read)
	}
}

func TestLoggerOmitsURLSecrets(t *testing.T) {
	var logs bytes.Buffer
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("failed at %s", req.URL)
	})
	_, _ = New(WithHTTPClient(&http.Client{Transport: transport}), fixedBackOff(0), WithMaxAttempts(2),
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))).
		GetBytes(t.Context(), "https://secret-user:secret-password@example.test/secret-path?secret-query")
	if got := logs.String(); !strings.Contains(got, "host=example.test") || strings.Contains(got, "secret-") {
		t.Fatalf("logs = %q", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type countedBody struct {
	io.Reader
	read   int
	closed bool
}

func (b *countedBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += n
	return n, err
}

func (b *countedBody) Close() error {
	b.closed = true
	return nil
}
