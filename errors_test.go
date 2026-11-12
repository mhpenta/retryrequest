package retryrequest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestStatusErrorCarriesHeaderAndBody(t *testing.T) {
	const problem = `{"error":"field x is required"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Request-Id", "req-123")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = io.WriteString(w, problem)
	}))
	defer server.Close()
	_, err := New().GetBytes(t.Context(), server.URL)
	var status *StatusError
	if !errors.As(err, &status) || status.StatusCode != 422 || string(status.Body) != problem ||
		status.Header.Get("X-Request-Id") != "req-123" {
		t.Fatalf("error = %#v", err)
	}
	if strings.Contains(err.Error(), "field x") {
		t.Fatalf("Error() includes the response body: %v", err)
	}
}

func TestStatusErrorBodyIsBounded(t *testing.T) {
	large := strings.Repeat("e", 100<<10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, large)
	}))
	defer server.Close()
	_, err := New().GetBytes(t.Context(), server.URL)
	var status *StatusError
	if !errors.As(err, &status) || !bytes.Equal(status.Body, []byte(large[:maxErrorBodyBytes])) {
		t.Fatalf("body length = %d, error = %v", len(status.Body), err)
	}
}

func TestStatusErrorBodyIsDecompressed(t *testing.T) {
	const problem = `{"error":"rate limited"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write(gzipBytes(t, []byte(problem)))
	}))
	defer server.Close()
	_, err := New().GetBytes(t.Context(), server.URL)
	var status *StatusError
	if !errors.As(err, &status) || string(status.Body) != problem {
		t.Fatalf("body = %q, error = %v", status.Body, err)
	}
}

// The error after retries describes the last response.
func TestRetryErrorKeepsLastResponse(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := calls.Add(1)
		w.Header().Set("X-Attempt", fmt.Sprint(n))
		w.WriteHeader(http.StatusBadGateway)
		_, _ = fmt.Fprintf(w, "upstream down, attempt %d", n)
	}))
	defer server.Close()
	_, err := New(fixedBackOff(0), WithMaxAttempts(3)).GetBytes(t.Context(), server.URL)
	var (
		retryErr *RetryError
		status   *StatusError
	)
	if !errors.As(err, &retryErr) || !errors.As(err, &status) ||
		string(status.Body) != "upstream down, attempt 3" || status.Header.Get("X-Attempt") != "3" {
		t.Fatalf("error = %v, body = %q", err, status.Body)
	}
}

// A predicate can decide from the error body, such as an API's error code.
func TestRetryPredicateCanReadErrorBody(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusConflict)
			_, _ = io.WriteString(w, `{"code":"lock_contention"}`)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	client := New(fixedBackOff(0), WithRetryIf(func(resp *http.Response, err error) bool {
		if resp != nil && resp.StatusCode == http.StatusConflict {
			body, _ := io.ReadAll(resp.Body)
			return bytes.Contains(body, []byte("lock_contention"))
		}
		return DefaultRetryIf(resp, err)
	}))
	got, err := client.GetBytes(t.Context(), server.URL)
	if err != nil || string(got) != "ok" || calls.Load() != 2 {
		t.Fatalf("body = %q, error = %v, attempts = %d", got, err, calls.Load())
	}
}
