package retryrequest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestBeforeAttemptSignsEachAttempt(t *testing.T) {
	const payload = `{"query":"filings"}`
	var (
		mu   sync.Mutex
		seen []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		mu.Lock()
		seen = append(seen, fmt.Sprintf("%s %v %s", req.Header.Get("Authorization"), req.Header.Values("X-Attempt"), body))
		n := len(seen)
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	client := New(fixedBackOff(0), WithRetryNonIdempotent(), WithBeforeAttempt(func(req *http.Request, attempt int) error {
		// Sign over the payload, read through GetBody so the attempt's body stays intact.
		body, err := req.GetBody()
		if err != nil {
			return err
		}
		defer body.Close()
		sum := sha256.New()
		if _, err := io.Copy(sum, body); err != nil {
			return err
		}
		req.Header.Set("Authorization", fmt.Sprintf("sig-%d-%s", attempt, hex.EncodeToString(sum.Sum(nil))[:8]))
		req.Header.Add("X-Attempt", fmt.Sprint(attempt))
		return nil
	}))
	got, err := client.PostBytes(t.Context(), server.URL, "application/json", []byte(payload))
	if err != nil || string(got) != "ok" {
		t.Fatalf("body = %q, error = %v", got, err)
	}
	digest := sha256.Sum256([]byte(payload))
	sig := hex.EncodeToString(digest[:])[:8]
	want := []string{
		fmt.Sprintf("sig-1-%s [1] %s", sig, payload),
		fmt.Sprintf("sig-2-%s [2] %s", sig, payload),
	}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("server saw %q, want %q", seen, want)
	}
}

func TestBeforeAttemptErrorStopsTheRequest(t *testing.T) {
	refresh := errors.New("token refresh failed")
	t.Run("on a retry", func(t *testing.T) {
		calls := 0
		_, err := New(WithHTTPClient(failingClient(&calls)), fixedBackOff(0), WithBeforeAttempt(func(_ *http.Request, attempt int) error {
			if attempt == 2 {
				return refresh
			}
			return nil
		})).GetBytes(t.Context(), "https://example.test")
		var status *StatusError
		if !errors.Is(err, refresh) || !errors.As(err, &status) || status.StatusCode != 503 || calls != 1 {
			t.Fatalf("error = %v, attempts sent = %d", err, calls)
		}
	})
	t.Run("before the first attempt", func(t *testing.T) {
		calls := 0
		body := &countedBody{Reader: strings.NewReader("payload")}
		req, err := http.NewRequest(http.MethodPut, "https://example.test", body)
		if err != nil {
			t.Fatal(err)
		}
		_, err = New(WithHTTPClient(failingClient(&calls)), WithBeforeAttempt(func(*http.Request, int) error { return refresh })).
			Do(t.Context(), req)
		if !errors.Is(err, refresh) || calls != 0 || !body.closed {
			t.Fatalf("error = %v, attempts sent = %d, body closed = %t", err, calls, body.closed)
		}
	})
}

func TestInvalidHeaderAndMethodAreNotRetried(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(*http.Request)
	}{
		{"header value", func(r *http.Request) { r.Header["X-Note"] = []string{"line one\nline two"} }},
		{"header name", func(r *http.Request) { r.Header["Bad Name"] = []string{"value"} }},
		{"method", func(r *http.Request) { r.Method = "BAD METHOD" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "http://example.test", bytes.NewReader(nil))
			if err != nil {
				t.Fatal(err)
			}
			tt.setup(req)
			attempts := 0
			_, err = New(fixedBackOff(0), WithRetryNonIdempotent(), WithBeforeAttempt(func(*http.Request, int) error {
				attempts++
				return nil
			})).Do(t.Context(), req)
			var retryErr *RetryError
			if err == nil || errors.As(err, &retryErr) || attempts != 1 || !IsPermanentTransportError(err) {
				t.Fatalf("error = %v, attempts = %d", err, attempts)
			}
		})
	}
}
