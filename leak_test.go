package retryrequest

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// connTracker counts server connections that are mid-request. A response body
// the client never closes keeps its connection, and that connection's
// goroutines, alive.
type connTracker struct {
	mu     sync.Mutex
	states map[net.Conn]http.ConnState
}

func (c *connTracker) set(conn net.Conn, state http.ConnState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state == http.StateClosed || state == http.StateHijacked {
		delete(c.states, conn)
		return
	}
	c.states[conn] = state
}

func (c *connTracker) active() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, state := range c.states {
		if state == http.StateActive {
			n++
		}
	}
	return n
}

// TestNoLeaks runs every success and failure path, then checks that closing
// idle connections returns goroutines to their starting count. Each leaked
// response body would keep about three goroutines alive, so one round of each
// path is enough to show a leak.
func TestNoLeaks(t *testing.T) {
	var flaky atomic.Int32
	gzipped := gzipBytes(t, []byte("hello"))
	tracker := &connTracker{states: map[net.Conn]http.ConnState{}}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			_, _ = io.WriteString(w, "ok")
		case "/503-then-ok":
			if flaky.Add(1)%2 == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				// Larger than the drain, so the connection cannot be reused.
				_, _ = io.WriteString(w, strings.Repeat("e", 2*maxDrainBytes))
				return
			}
			_, _ = io.WriteString(w, "ok")
		case "/502":
			w.WriteHeader(http.StatusBadGateway)
		case "/404":
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":"missing"}`)
		case "/truncated":
			w.Header().Set("Content-Length", "1000")
			_, _ = io.WriteString(w, "partial")
		case "/large":
			_, _ = io.WriteString(w, strings.Repeat("x", 4096))
		case "/bad-gzip":
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = io.WriteString(w, "not gzip")
		case "/gzip":
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write(gzipped)
		case "/slow":
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}
	}))
	server.Config.ConnState = tracker.set
	server.Start()
	defer server.Close()

	transport := &http.Transport{DisableCompression: true}
	defer transport.CloseIdleConnections()
	newClient := func(opts ...Option) *Client {
		return New(append([]Option{
			WithHTTPClient(&http.Client{Transport: transport}),
			WithMaxRetryWait(time.Millisecond),
			WithMaxAttempts(3),
		}, opts...)...)
	}
	plain := newClient()
	converting := newClient(WithUTF8Conversion(), WithMaxBodyBytes(1024))
	timed := newClient(WithPerAttemptTimeout(50 * time.Millisecond))
	hooked := newClient(WithBeforeAttempt(func(_ *http.Request, attempt int) error {
		if attempt == 2 {
			return errors.New("refused")
		}
		return nil
	}))
	ctx := t.Context()
	url := func(path string) string { return server.URL + path }

	baseline := runtime.NumGoroutine()
	for range 3 {
		// Buffered helpers own every body.
		_, _ = plain.GetBytes(ctx, url("/ok"))
		_, _ = plain.GetBytes(ctx, url("/503-then-ok"))
		_, _ = plain.GetBytes(ctx, url("/502"))
		_, _ = plain.GetBytes(ctx, url("/404"))
		_, _ = plain.GetBytes(ctx, url("/truncated"))
		_, _ = plain.PostBytes(ctx, url("/502"), "text/plain", []byte("payload"))
		_, _ = converting.GetBytes(ctx, url("/large"))
		_, _ = converting.GetBytes(ctx, url("/bad-gzip"))
		_, _ = converting.GetBytes(ctx, url("/gzip"))
		_, _ = timed.GetBytes(ctx, url("/slow"))
		_, _ = hooked.GetBytes(ctx, url("/502"))

		// Cancellation during a wait and during a request.
		waiting, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		_, _ = newClient(WithMaxRetryWait(time.Second)).GetBytes(waiting, url("/502"))
		cancel()
		sending, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		_, _ = plain.GetBytes(sending, url("/slow"))
		cancel()

		// Streams and Do hand a successful body to the caller, who closes it.
		if body, err := converting.GetReader(ctx, url("/large")); err == nil {
			_, _ = io.Copy(io.Discard, body)
			_ = body.Close()
		}
		if body, err := timed.GetReader(ctx, url("/ok")); err == nil {
			_ = body.Close()
		}
		for _, path := range []string{"/ok", "/404"} {
			req, err := http.NewRequest(http.MethodGet, url(path), nil)
			if err != nil {
				t.Fatal(err)
			}
			if resp, err := plain.Do(ctx, req); err == nil {
				_ = resp.Body.Close()
			}
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		transport.CloseIdleConnections()
		extra, active := runtime.NumGoroutine()-baseline, tracker.active()
		if extra <= 0 && active == 0 {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			t.Fatalf("leaked %d goroutines and %d active connections:\n%s", extra, active, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(20 * time.Millisecond)
	}
}
