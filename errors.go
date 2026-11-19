package retryrequest

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

var (
	errRetry         = errors.New("retryable HTTP failure")
	errRateLimitWait = errors.New("waiting for rate limit")
)

// ErrBodyTooLarge identifies a response that exceeds the helper output limit.
var ErrBodyTooLarge = errors.New("HTTP response body too large")

// BodyTooLargeError describes the limit that was exceeded.
type BodyTooLargeError struct {
	Limit int64
}

func (e *BodyTooLargeError) Error() string {
	return fmt.Sprintf("%s: limit is %d bytes", ErrBodyTooLarge, e.Limit)
}

func (e *BodyTooLargeError) Unwrap() error { return ErrBodyTooLarge }

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
