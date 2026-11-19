package retryrequest

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestRateLimitingAcrossCallsRetriesAndRedirects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var sent []time.Duration
		transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			sent = append(sent, time.Since(start))
			resp := &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}
			switch len(sent) {
			case 1:
				resp.StatusCode = http.StatusServiceUnavailable
			case 2:
				resp.StatusCode = http.StatusFound
				resp.Header = http.Header{"Location": {"/redirected"}}
			}
			return resp, nil
		})
		redirects := 0
		base := &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				redirects++
				return nil
			},
		}
		limit := WithRateLimiting(2, 1)
		first := New(limit, WithHTTPClient(base), fixedBackOff(0))
		second := New(WithHTTPClient(base), limit)
		for _, client := range []*Client{first, first, second, second} {
			if _, err := client.GetBytes(t.Context(), "https://example.test"); err != nil {
				t.Fatal(err)
			}
		}
		// The second client starts with its own full bucket.
		want := []time.Duration{0, 500 * time.Millisecond, time.Second, 1500 * time.Millisecond, 1500 * time.Millisecond, 2 * time.Second}
		if !slices.Equal(sent, want) || redirects != 1 {
			t.Fatalf("round trips at %v, want %v; redirects = %d", sent, want, redirects)
		}
		if _, ok := base.Transport.(roundTripFunc); !ok {
			t.Fatal("rate limiting replaced the caller's transport")
		}
	})
}

func TestRateLimitingSharedBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := make(chan time.Time, 3)
		client := New(WithRateLimiting(1, 2), WithHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls <- time.Now()
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})}))
		start := time.Now()
		for range 3 {
			go func() {
				if _, err := client.GetBytes(t.Context(), "https://example.test"); err != nil {
					t.Error(err)
				}
			}()
		}
		for _, want := range []time.Duration{0, 0, time.Second} {
			if got := (<-calls).Sub(start); got != want {
				t.Fatalf("round trip at %s, want %s", got, want)
			}
		}
	})
}

func TestRateLimitWaitDeadlinePreservesLastFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		client := New(WithHTTPClient(statusClient(&calls, http.StatusServiceUnavailable, "")),
			WithRateLimiting(1, 1), fixedBackOff(0))
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		defer cancel()
		_, err := client.GetBytes(ctx, "https://example.test")
		var status *StatusError
		if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &status) || status.StatusCode != 503 || calls != 1 {
			t.Fatalf("error = %v, round trips = %d", err, calls)
		}
	})
}

func TestRateLimitCancelClosesQueuedRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		client := New(WithRateLimiting(1, 1), WithHTTPClient(statusClient(&calls, http.StatusOK, "")))
		if _, err := client.GetBytes(t.Context(), "https://example.test"); err != nil {
			t.Fatal(err)
		}
		body := &countedBody{Reader: strings.NewReader("payload")}
		req, err := http.NewRequest(http.MethodPut, "https://example.test", body)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(time.Millisecond, cancel)
		_, err = client.Do(ctx, req)
		if !errors.Is(err, context.Canceled) || calls != 1 || !body.closed || body.read != 0 {
			t.Fatalf("error = %v, round trips = %d, queued body = %+v", err, calls, body)
		}
	})
}
