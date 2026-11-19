package retryrequest

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// statusClient answers every request with code and, if set, Retry-After.
func statusClient(calls *int, code int, retryAfter string) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		*calls++
		header := http.Header{}
		if retryAfter != "" {
			header.Set("Retry-After", retryAfter)
		}
		return &http.Response{StatusCode: code, Header: header, Body: http.NoBody}, nil
	})}
}

func TestCustomBackOffWaitCap(t *testing.T) {
	for _, tt := range []struct {
		name     string
		opts     []Option
		attempts int
		elapsed  time.Duration
		reason   string
	}{
		{"uncapped by default", []Option{WithMaxElapsedTime(time.Hour)}, 2, time.Minute, ""},
		{"capped when set", []Option{WithMaxElapsedTime(time.Hour), WithMaxRetryWait(10 * time.Second)}, 2, 10 * time.Second, ""},
		// The default 30-second budget still bounds an uncapped wait.
		{"bounded by elapsed time", nil, 1, 0, "maximum retry elapsed time exceeded"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				opts := append([]Option{
					WithHTTPClient(statusClient(&calls, http.StatusBadGateway, "")),
					WithMaxAttempts(2),
					fixedBackOff(time.Minute),
				}, tt.opts...)
				start := time.Now()
				_, err := New(opts...).GetBytes(t.Context(), "https://example.test")
				var retryErr *RetryError
				if !errors.As(err, &retryErr) || calls != tt.attempts || time.Since(start) != tt.elapsed || retryErr.Reason != tt.reason {
					t.Fatalf("error = %v, attempts = %d, elapsed = %s", err, calls, time.Since(start))
				}
			})
		})
	}
}

func TestRetryAfterWithCustomBackOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Uncapped, a two-minute Retry-After is honored within the budget.
		calls := 0
		start := time.Now()
		_, _ = New(WithHTTPClient(statusClient(&calls, http.StatusServiceUnavailable, "120")),
			WithMaxAttempts(2), WithMaxElapsedTime(time.Hour), fixedBackOff(time.Second)).
			GetBytes(t.Context(), "https://example.test")
		if calls != 2 || time.Since(start) != 2*time.Minute {
			t.Fatalf("uncapped: attempts = %d, elapsed = %s", calls, time.Since(start))
		}

		// With an explicit cap, the same Retry-After ends retries at once.
		calls = 0
		_, err := New(WithHTTPClient(statusClient(&calls, http.StatusServiceUnavailable, "120")),
			WithMaxAttempts(2), WithMaxElapsedTime(time.Hour), WithMaxRetryWait(10*time.Second), fixedBackOff(time.Second)).
			GetBytes(t.Context(), "https://example.test")
		if calls != 1 || err == nil || !strings.Contains(err.Error(), "Retry-After exceeds maximum wait") {
			t.Fatalf("capped: attempts = %d, error = %v", calls, err)
		}
	})
}

func TestStatusWaitOptions(t *testing.T) {
	const base = 4 * time.Second
	for _, tt := range []struct {
		name   string
		code   int
		header string
		option Option
		want   time.Duration
	}{
		{"429 minimum", 429, "", WithLongBackOffOn429(9 * time.Second), 9 * time.Second},
		{"429 shorter header", 429, "2", WithLongBackOffOn429(9 * time.Second), 9 * time.Second},
		{"429 longer header", 429, "12", WithLongBackOffOn429(9 * time.Second), 12 * time.Second},
		{"429 longer backoff", 429, "", WithLongBackOffOn429(time.Second), base},
		{"503 missing header", 503, "", WithWaitOn503(2 * time.Second), 2 * time.Second},
		{"503 invalid header", 503, "later", WithWaitOn503(2 * time.Second), 2 * time.Second},
		{"503 server delay", 503, "1", WithWaitOn503(2 * time.Second), time.Second},
		{"503 explicit zero", 503, "0", WithWaitOn503(2 * time.Second), 0},
		{"503 option disabled", 503, "0", WithWaitOn503(0), base},
		{"other status", 502, "7", WithWaitOn503(2 * time.Second), base},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				client := New(WithHTTPClient(statusClient(&calls, tt.code, tt.header)), fixedBackOff(base), WithMaxAttempts(2), tt.option)
				start := time.Now()
				_, err := client.GetBytes(t.Context(), "https://example.test")
				var status *StatusError
				if calls != 2 || time.Since(start) != tt.want || !errors.As(err, &status) {
					t.Fatalf("calls = %d, elapsed = %s, error = %v", calls, time.Since(start), err)
				}
				if status.Header.Get("Retry-After") != tt.header || status.RetryAfter != parseRetryAfter(tt.header, start) {
					t.Fatalf("reported server Retry-After = %q (%s)", status.Header.Get("Retry-After"), status.RetryAfter)
				}
			})
		})
	}
}

func TestConfiguredStatusWaitExceedsCap(t *testing.T) {
	calls := 0
	client := New(WithHTTPClient(statusClient(&calls, 503, "")), WithWaitOn503(2*time.Minute))
	_, err := client.GetBytes(t.Context(), "https://example.test")
	var exhausted *RetryError
	if !errors.As(err, &exhausted) || calls != 1 || !strings.Contains(exhausted.Reason, "configured 503 wait") {
		t.Fatalf("error = %v, calls = %d", err, calls)
	}
}
