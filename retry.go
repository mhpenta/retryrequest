package retryrequest

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v7"
)

var errRetry = errors.New("retryable HTTP failure")

// maxErrorBodyBytes bounds the response body kept in a StatusError.
const maxErrorBodyBytes = 4 << 10

// StatusError describes a non-2xx HTTP response. RetryAfter is negative when
// the header is absent or invalid. Header is the response's header. Body holds
// up to the first 4 KiB of the response body, decompressed when the response
// is gzip-encoded, so a caller can read the server's explanation. Error does
// not include Body, so logging the error does not log response content.
type StatusError struct {
	StatusCode int
	Host       string
	RetryAfter time.Duration
	Header     http.Header
	Body       []byte
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("HTTP request to %s returned status %d %s", e.Host, e.StatusCode, http.StatusText(e.StatusCode))
}

// RetryError is returned when the client stops retrying a failure that was
// retryable: the attempt limit or elapsed-time budget ran out, a Retry-After
// exceeded the wait cap, or the next wait would pass the context's deadline.
// Attempts counts every attempt made. Reason describes an early stop, and Err
// is the last failure. A failure that is not retryable is returned directly.
type RetryError struct {
	Attempts int
	Reason   string
	Err      error
}

func (e *RetryError) Error() string {
	message := fmt.Sprintf("HTTP request failed after %d attempts: %v", e.Attempts, e.Err)
	if e.Reason != "" {
		message += " (" + e.Reason + ")"
	}
	return message
}

func (e *RetryError) Unwrap() error { return e.Err }

// DefaultRetryIf reports whether a transport failure, HTTP status, or buffered
// body read error is retryable. It can be composed with WithRetryIf.
func DefaultRetryIf(resp *http.Response, err error) bool {
	if err != nil {
		if resp == nil {
			return !terminalError(err) && !IsPermanentTransportError(err)
		}
		return resp.StatusCode >= 200 && resp.StatusCode < 300 && !permanentReadError(err)
	}
	if resp == nil {
		return false
	}
	switch resp.StatusCode {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests:
		return true
	case http.StatusNotImplemented, http.StatusHTTPVersionNotSupported, http.StatusNetworkAuthenticationRequired:
		return false
	default:
		return resp.StatusCode >= 500 && resp.StatusCode < 600
	}
}

// IsPermanentTransportError reports transport failures that a retry cannot fix:
// a certificate the client rejects, a TLS connection to a server that does not
// speak TLS, a URL scheme the transport does not support, or a header or method
// that net/http rejects before sending. DNS "no such host" is deliberately not
// included, because some resolvers report it while offline.
func IsPermanentTransportError(err error) bool {
	var (
		verification     *tls.CertificateVerificationError
		unknownAuthority x509.UnknownAuthorityError
		hostname         x509.HostnameError
		invalid          x509.CertificateInvalidError
		record           tls.RecordHeaderError
	)
	if errors.Is(err, http.ErrSchemeMismatch) || errors.As(err, &verification) ||
		errors.As(err, &unknownAuthority) || errors.As(err, &hostname) ||
		errors.As(err, &invalid) || errors.As(err, &record) {
		return true
	}
	// net/http has no sentinels for these. Match the root cause so URL text
	// from a wrapping error cannot affect classification.
	for cause := errors.Unwrap(err); cause != nil; cause = errors.Unwrap(cause) {
		err = cause
	}
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.HasPrefix(message, `unsupported protocol scheme "`) ||
		strings.HasPrefix(message, "net/http: invalid header ") ||
		strings.HasPrefix(message, "net/http: invalid method ")
}

func terminalError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, ErrBodyTooLarge) || errors.Is(err, backoff.ErrPermanent)
}

// permanentReadError reports body failures that would recur on a fresh copy of
// the same response: an oversized body or a body that is not gzip at all.
// Truncation and connection failures mid-body are retryable.
func permanentReadError(err error) bool {
	return terminalError(err) || errors.Is(err, gzip.ErrHeader)
}

// responseError describes a failed response from the body bytes already read.
func responseError(resp *http.Response, host string, raw []byte) *StatusError {
	if resp.Request != nil && resp.Request.URL != nil {
		host = resp.Request.URL.Hostname()
	}
	return &StatusError{
		StatusCode: resp.StatusCode,
		Host:       host,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		Header:     resp.Header,
		Body:       errorBodyExcerpt(raw, resp.Header),
	}
}

// errorBodyExcerpt returns the start of a failed response's body. A gzip body
// is decompressed as far as the bytes read allow; if that yields nothing, the
// raw bytes are kept.
func errorBodyExcerpt(raw []byte, header http.Header) []byte {
	if len(raw) == 0 {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(header.Get("Content-Encoding")), "gzip") {
		if reader, err := gzip.NewReader(bytes.NewReader(raw)); err == nil {
			// A read error after some output only means the excerpt is partial.
			decoded, _ := io.ReadAll(io.LimitReader(reader, maxErrorBodyBytes))
			if len(decoded) > 0 {
				return decoded
			}
		}
	}
	return bytes.Clone(raw[:min(len(raw), maxErrorBodyBytes)])
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseUint(value, 10, 32); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(0, at.Sub(now))
	}
	return -1
}

func (c *Client) defaultBackOff() backoff.BackOff {
	// Jitter over half to all of the base delay, with a one-second initial
	// base that doubles up to maxWait. The midpoint is 3/4 of the base.
	midpoint := c.maxWait - c.maxWait/4
	return &backoff.ExponentialBackOff{
		InitialInterval:     min(750*time.Millisecond, midpoint),
		RandomizationFactor: 1.0 / 3,
		Multiplier:          2,
		MaxInterval:         midpoint,
	}
}

// A status wait is a minimum unless the caller selects a fixed wait for 503.
type statusWait struct {
	delay  time.Duration
	source string
	exact  bool
}

func (c *Client) waitForStatus(status *StatusError) statusWait {
	switch status.StatusCode {
	case http.StatusTooManyRequests:
		if c.minWaitOn429 > status.RetryAfter {
			return statusWait{delay: c.minWaitOn429, source: "configured 429 minimum"}
		}
	case http.StatusServiceUnavailable:
		if c.waitOn503 > 0 {
			if status.RetryAfter >= 0 {
				return statusWait{delay: status.RetryAfter, source: "Retry-After", exact: true}
			}
			return statusWait{delay: c.waitOn503, source: "configured 503 wait", exact: true}
		}
	default:
		return statusWait{}
	}
	return statusWait{delay: status.RetryAfter, source: "Retry-After"}
}

// retrySchedule applies the status policy and request limits to each backoff.
type retrySchedule struct {
	backoff.BackOff
	ctx        context.Context
	maxWait    time.Duration
	capped     bool
	statusWait statusWait
	stopReason string
}

func (s *retrySchedule) NextBackOff() time.Duration {
	if s.capped && s.statusWait.delay > s.maxWait {
		s.stopReason = fmt.Sprintf("%s exceeds maximum wait of %s", s.statusWait.source, s.maxWait)
		return backoff.Stop
	}
	next := s.BackOff.NextBackOff()
	if next == backoff.Stop {
		return backoff.Stop
	}
	if s.capped {
		next = min(next, s.maxWait)
	}
	wait := max(0, next, s.statusWait.delay)
	if s.statusWait.exact {
		wait = s.statusWait.delay
	}
	if deadline, ok := s.ctx.Deadline(); ok && time.Until(deadline) <= wait {
		s.stopReason = fmt.Sprintf("retry wait of %s would reach the context deadline", wait)
		return backoff.Stop
	}
	return wait
}
