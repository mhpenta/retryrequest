package retryrequest

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
)

func TestNonIdempotentMethodsAreSentOnce(t *testing.T) {
	for _, tt := range []struct {
		name       string
		method     string
		header     string
		value      string
		configured string
		want       int32
	}{
		{"POST", http.MethodPost, "", "", "", 1},
		{"PATCH", http.MethodPatch, "", "", "", 1},
		{"standard key", http.MethodPost, "Idempotency-Key", "caller-key", "", 2},
		{"alternate key", http.MethodPost, "X-Idempotency-Key", "caller-key", "", 2},
		{"custom key", http.MethodPost, "X-Operation-ID", "caller-key", "X-Operation-ID", 2},
		{"blank standard key", http.MethodPost, "Idempotency-Key", " \t ", "", 1},
		{"blank alternate key", http.MethodPost, "X-Idempotency-Key", "", "", 1},
		{"PUT", http.MethodPut, "", "", "", 2},
		{"DELETE", http.MethodDelete, "", "", "", 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if tt.header != "" {
					var want []string
					if key := strings.TrimSpace(tt.value); key != "" {
						want = []string{key}
					}
					if got := req.Header.Values(tt.header); !slices.Equal(got, want) {
						t.Errorf("%s on wire = %q, want %q", tt.header, got, want)
					}
				}
				if calls.Add(1) == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				_, _ = io.WriteString(w, "ok")
			}))
			defer server.Close()
			req, err := http.NewRequest(tt.method, server.URL, strings.NewReader("write"))
			if err != nil {
				t.Fatal(err)
			}
			if tt.header != "" {
				req.Header.Set(tt.header, tt.value)
			}
			resp, err := New(fixedBackOff(0), WithIdempotencyKeyHeader(tt.configured)).Do(t.Context(), req)
			if resp != nil {
				resp.Body.Close()
			}
			if calls.Load() != tt.want || (tt.want == 1) != (err != nil) {
				t.Fatalf("attempts = %d, want %d, error = %v", calls.Load(), tt.want, err)
			}
			if tt.header != "" && req.Header.Get(tt.header) != tt.value {
				t.Errorf("original request header was changed: %q", req.Header.Get(tt.header))
			}
		})
	}
}

// A connection that drops after the server read the request is the case that
// duplicates writes: the client cannot tell whether the POST was applied.
func TestPostNotResentAfterConnectionDrop(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		_, _ = io.ReadAll(req.Body)
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer server.Close()
	_, err := New(fixedBackOff(0)).PostBytes(t.Context(), server.URL, "application/json", []byte(`{"amount":1}`))
	if err == nil || calls.Load() != 1 {
		t.Fatalf("attempts = %d, error = %v", calls.Load(), err)
	}
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestIdempotencyKey(t *testing.T) {
	var (
		mu   sync.Mutex
		keys []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		keys = append(keys, req.Header.Get("Idempotency-Key"))
		n := len(keys)
		mu.Unlock()
		if n%2 == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	client := New(fixedBackOff(0), WithIdempotencyKeyHeader("idempotency-key"))

	for _, client := range []*Client{
		client,
		New(fixedBackOff(0), WithHeader("Idempotency-Key", "default-key"), WithIdempotencyKeyHeader("Idempotency-Key")),
	} {
		if got, err := client.PostBytes(t.Context(), server.URL, "application/json", []byte("{}")); err != nil || string(got) != "ok" {
			t.Fatalf("body = %q, error = %v", got, err)
		}
	}
	if len(keys) != 4 || keys[0] != keys[1] || keys[2] != keys[3] || keys[0] == keys[2] || !uuidV4.MatchString(keys[0]) {
		t.Fatalf("keys = %q, want one UUID per call, repeated on its retry", keys)
	}

	keys = nil
	req, err := http.NewRequest(http.MethodPost, server.URL, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Idempotency-Key", "caller-key")
	resp, err := client.Do(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if fmt.Sprint(keys) != "[caller-key caller-key]" {
		t.Fatalf("keys = %q, want the caller's key kept", keys)
	}

	keys = nil
	if _, err := client.GetBytes(t.Context(), server.URL); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(keys) != "[ ]" {
		t.Fatalf("keys = %q, want none on GET", keys)
	}
}

// countConns counts TCP connections, which is one per attempt when a handshake fails.
func countConns(server *httptest.Server) *atomic.Int32 {
	var conns atomic.Int32
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	return &conns
}

func TestPermanentTransportErrorsAreNotRetried(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	t.Run("untrusted certificate", func(t *testing.T) {
		server := httptest.NewUnstartedServer(handler)
		conns := countConns(server)
		server.StartTLS()
		defer server.Close()
		_, err := New(WithHTTPClient(&http.Client{}), fixedBackOff(0)).GetBytes(t.Context(), server.URL)
		var verification *tls.CertificateVerificationError
		if !errors.As(err, &verification) || conns.Load() != 1 {
			t.Fatalf("connections = %d, error = %v", conns.Load(), err)
		}
	})
	t.Run("TLS to a plain HTTP server", func(t *testing.T) {
		server := httptest.NewUnstartedServer(handler)
		conns := countConns(server)
		server.Start()
		defer server.Close()
		_, err := New(WithHTTPClient(&http.Client{}), fixedBackOff(0)).GetBytes(t.Context(), strings.Replace(server.URL, "http:", "https:", 1))
		if !errors.Is(err, http.ErrSchemeMismatch) || conns.Load() != 1 {
			t.Fatalf("connections = %d, error = %v", conns.Load(), err)
		}
	})
	t.Run("unsupported scheme", func(t *testing.T) {
		_, err := New(fixedBackOff(0)).GetBytes(t.Context(), "ftp://example.test/file")
		var exhausted *RetryError
		if err == nil || errors.As(err, &exhausted) {
			t.Fatalf("error = %v, want a single failed attempt", err)
		}
	})
}

func TestPermanentTransportErrorClassification(t *testing.T) {
	wrap := func(err error) error { return &url.Error{Op: "Get", URL: "https://example.test", Err: err} }
	for _, tt := range []struct {
		name      string
		err       error
		permanent bool
	}{
		{"unknown authority", wrap(x509.UnknownAuthorityError{}), true},
		{"hostname", wrap(x509.HostnameError{Certificate: &x509.Certificate{}, Host: "example.test"}), true},
		{"expired", wrap(x509.CertificateInvalidError{Cert: &x509.Certificate{}, Reason: x509.Expired}), true},
		{"connection reset", wrap(&net.OpError{Op: "read", Err: errors.New("connection reset by peer")}), false},
		{"unexpected EOF", wrap(io.ErrUnexpectedEOF), false},
		{"scheme mismatch", wrap(http.ErrSchemeMismatch), true},
		{"unsupported scheme", wrap(fmt.Errorf("transport: %w", errors.New(`unsupported protocol scheme "ftp"`))), true},
		{"error text in URL", fmt.Errorf("fetch: %w", &url.Error{
			Op: "Get", URL: "https://example.test/?q=unsupported protocol scheme", Err: io.ErrUnexpectedEOF,
		}), false},
		// Some resolvers report "no such host" while offline, so it stays retryable.
		{"no such host", wrap(&net.DNSError{Err: "no such host", Name: "example.test", IsNotFound: true}), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanentTransportError(tt.err); got != tt.permanent {
				t.Fatalf("IsPermanentTransportError = %t, want %t", got, tt.permanent)
			}
			if got := DefaultRetryIf(nil, tt.err); got == tt.permanent {
				t.Fatalf("DefaultRetryIf = %t", got)
			}
		})
	}
}

func TestBufferedReadUsesRetryPolicy(t *testing.T) {
	wireLimit := errors.New("transport body limit exceeded")
	for _, tt := range []struct {
		name string
		err  error
		want int
	}{
		{"transport limit", fmt.Errorf("read wire body: %w", wireLimit), 1},
		{"truncated body", io.ErrUnexpectedEOF, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls, decisions := 0, 0
			failed := &countedBody{Reader: iotest.ErrReader(tt.err)}
			transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					return &http.Response{StatusCode: http.StatusOK, Body: failed}, nil
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok"))}, nil
			})
			client := New(fixedBackOff(0), WithHTTPClient(&http.Client{Transport: transport}),
				WithRetryIf(func(resp *http.Response, err error) bool {
					decisions++
					if resp == nil || resp.StatusCode != http.StatusOK || !errors.Is(err, tt.err) {
						t.Fatalf("retry predicate got response %v, error %v", resp, err)
					}
					return !errors.Is(err, wireLimit) && DefaultRetryIf(resp, err)
				}))
			got, err := client.GetBytes(t.Context(), "https://example.test")
			if calls != tt.want || decisions != 1 || !failed.closed {
				t.Fatalf("attempts = %d, decisions = %d, closed = %t", calls, decisions, failed.closed)
			}
			if tt.want == 1 {
				if !errors.Is(err, wireLimit) || got != nil {
					t.Fatalf("body = %q, error = %v, want transport limit", got, err)
				}
			} else if err != nil || string(got) != "ok" {
				t.Fatalf("body = %q, error = %v", got, err)
			}
		})
	}
}

// truncatingServer cuts the first `failures` responses short after promising
// the full Content-Length, then serves the whole body.
func truncatingServer(t *testing.T, failures int32, header http.Header, body []byte) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for key, values := range header {
			w.Header()[key] = values
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		if calls.Add(1) <= failures {
			_, _ = w.Write(body[:len(body)/2])
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	return server, &calls
}

func TestBufferedHelpersRetryTruncatedBodies(t *testing.T) {
	payload := bytes.Repeat([]byte("observation,"), 1000)
	t.Run("plain", func(t *testing.T) {
		server, calls := truncatingServer(t, 1, nil, payload)
		got, err := New(fixedBackOff(0)).GetBytes(t.Context(), server.URL)
		if err != nil || !bytes.Equal(got, payload) || calls.Load() != 2 {
			t.Fatalf("received %d bytes, error = %v, attempts = %d", len(got), err, calls.Load())
		}
	})
	t.Run("gzip", func(t *testing.T) {
		server, calls := truncatingServer(t, 1, http.Header{"Content-Encoding": {"gzip"}, "Content-Type": {"text/plain"}}, gzipBytes(t, payload))
		got, err := New(fixedBackOff(0)).GetBytes(t.Context(), server.URL)
		if err != nil || !bytes.Equal(got, payload) || calls.Load() != 2 {
			t.Fatalf("received %d bytes, error = %v, attempts = %d", len(got), err, calls.Load())
		}
	})
	// Text shorter than charset's 1KB read-ahead must not come back half-read as success.
	t.Run("short text with UTF-8 conversion", func(t *testing.T) {
		short := []byte("date,value\n2026-11-12,4.25\n")
		server, calls := truncatingServer(t, 1, http.Header{"Content-Type": {"text/csv; charset=utf-8"}}, short)
		got, err := New(fixedBackOff(0), WithUTF8Conversion()).GetBytes(t.Context(), server.URL)
		if err != nil || !bytes.Equal(got, short) || calls.Load() != 2 {
			t.Fatalf("body = %q, error = %v, attempts = %d", got, err, calls.Load())
		}
	})
	t.Run("exhausted", func(t *testing.T) {
		server, calls := truncatingServer(t, 10, nil, payload)
		_, err := New(fixedBackOff(0), WithMaxAttempts(3)).GetBytes(t.Context(), server.URL)
		var exhausted *RetryError
		if !errors.As(err, &exhausted) || exhausted.Attempts != 3 || !errors.Is(err, io.ErrUnexpectedEOF) || calls.Load() != 3 {
			t.Fatalf("error = %v, attempts = %d", err, calls.Load())
		}
	})
	t.Run("POST is not resent", func(t *testing.T) {
		server, calls := truncatingServer(t, 1, nil, payload)
		_, err := New(fixedBackOff(0)).PostBytes(t.Context(), server.URL, "application/json", []byte("{}"))
		if !errors.Is(err, io.ErrUnexpectedEOF) || calls.Load() != 1 {
			t.Fatalf("error = %v, attempts = %d", err, calls.Load())
		}
	})
	t.Run("stream is not restarted", func(t *testing.T) {
		server, calls := truncatingServer(t, 1, nil, payload)
		reader, err := New(fixedBackOff(0)).GetReader(t.Context(), server.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		if _, err := io.ReadAll(reader); !errors.Is(err, io.ErrUnexpectedEOF) || calls.Load() != 1 {
			t.Fatalf("error = %v, attempts = %d", err, calls.Load())
		}
	})
}

func TestEmptyGzipBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
	}))
	defer server.Close()
	got, err := New(fixedBackOff(0)).GetBytes(t.Context(), server.URL)
	if err != nil || len(got) != 0 {
		t.Fatalf("body = %q, error = %v", got, err)
	}
}
