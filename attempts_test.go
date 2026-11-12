package retryrequest

import (
	"errors"
	"net/http"
	"testing"
	"testing/synctest"
	"time"
)

func failingClient(calls *int) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		*calls++
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: http.NoBody}, nil
	})}
}

func TestMaxAttemptsBelowOneMeansOneAttempt(t *testing.T) {
	for _, n := range []int{0, -1, 1} {
		calls := 0
		_, err := New(WithHTTPClient(failingClient(&calls)), WithMaxAttempts(n), fixedBackOff(0)).
			GetBytes(t.Context(), "https://example.test")
		var retryErr *RetryError
		if !errors.As(err, &retryErr) || retryErr.Attempts != 1 || calls != 1 {
			t.Fatalf("WithMaxAttempts(%d): error = %v, attempts = %d", n, err, calls)
		}
	}
}

// Unlimited attempts still stop at the elapsed-time budget.
func TestUnlimitedAttemptsStopAtElapsedTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		start := time.Now()
		_, err := New(WithHTTPClient(failingClient(&calls)), WithUnlimitedAttempts(),
			WithMaxElapsedTime(time.Hour), WithMaxRetryWait(time.Minute), fixedBackOff(time.Minute)).GetBytes(t.Context(), "https://example.test")
		var retryErr *RetryError
		if !errors.As(err, &retryErr) || calls < 60 || time.Since(start) > time.Hour {
			t.Fatalf("error = %v, attempts = %d, elapsed = %s", err, calls, time.Since(start))
		}
	})
}
