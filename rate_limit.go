package retryrequest

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"golang.org/x/time/rate"
)

var errRateLimitWait = errors.New("waiting for rate limit")

type rateLimitedTransport struct {
	next    http.RoundTripper
	limiter *rate.Limiter
}

func withRateLimitedTransport(client *http.Client, limiter *rate.Limiter) *http.Client {
	// The caller or another retry client may also use the original client.
	limited := *client
	next := limited.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	limited.Transport = &rateLimitedTransport{next: next, limiter: limiter}
	return &limited
}

func (t *rateLimitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	if err := t.limiter.Wait(ctx); err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		// Each reservation needs one token and fits the configured burst.
		// Wait can also refuse a reservation before the deadline expires.
		cause := ctx.Err()
		if cause == nil {
			cause = context.DeadlineExceeded
		}
		return nil, fmt.Errorf("%w: %w", errRateLimitWait, cause)
	}
	return t.next.RoundTrip(req)
}

// CancelRequest preserves http.Client.Timeout for legacy transports that use
// this hook to interrupt an in-flight round trip.
func (t *rateLimitedTransport) CancelRequest(req *http.Request) {
	if transport, ok := t.next.(interface{ CancelRequest(*http.Request) }); ok {
		transport.CancelRequest(req)
	}
}
