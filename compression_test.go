package retryrequest

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// gzipServer serves payload gzip-compressed when the request accepts gzip,
// and records each request's Accept-Encoding.
func gzipServer(t *testing.T, payload []byte) (*httptest.Server, func() []string) {
	t.Helper()
	compressed := gzipBytes(t, payload)
	var (
		mu       sync.Mutex
		accepted []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		accepted = append(accepted, req.Header.Get("Accept-Encoding"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; charset=iso-8859-1")
		if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = w.Write(compressed)
			return
		}
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)
	return server, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), accepted...)
	}
}

func TestGzipIsRequestedAndDecompressedByDefault(t *testing.T) {
	payload := []byte(strings.Repeat("caf\xe9 ", 100)) // Latin-1, left unconverted
	for _, tt := range []struct {
		name   string
		client *http.Client
	}{
		{"default client", nil},
		// An injected client that disables compression, as quipu's public client does.
		{"compression disabled", &http.Client{Transport: &http.Transport{DisableCompression: true}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server, accepted := gzipServer(t, payload)
			client := New(WithHTTPClient(tt.client))

			got, err := client.GetBytes(t.Context(), server.URL)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("GetBytes = %q, error = %v", got, err)
			}
			reader, err := client.GetReader(t.Context(), server.URL)
			if err != nil {
				t.Fatal(err)
			}
			streamed, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil || !bytes.Equal(streamed, payload) {
				t.Fatalf("GetReader = %q, error = %v", streamed, err)
			}
			for _, value := range accepted() {
				if value != "gzip" {
					t.Fatalf("Accept-Encoding = %q, want gzip on every helper request", value)
				}
			}
		})
	}
}

// Do behaves like http.Client: the default client's transport requests and
// decompresses gzip transparently.
func TestDoUsesTransparentGzip(t *testing.T) {
	payload := []byte("plain response")
	server, accepted := gzipServer(t, payload)
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := New().Do(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(got, payload) || !resp.Uncompressed || accepted()[0] != "gzip" {
		t.Fatalf("body = %q, error = %v, uncompressed = %t, accepted = %q", got, err, resp.Uncompressed, accepted())
	}
}

// A caller that chooses its own Accept-Encoding gets what it asked for.
func TestCallerAcceptEncodingIsKept(t *testing.T) {
	server, accepted := gzipServer(t, []byte("identity please"))
	got, err := New(WithHeader("Accept-Encoding", "identity")).GetBytes(t.Context(), server.URL)
	if err != nil || string(got) != "identity please" || accepted()[0] != "identity" {
		t.Fatalf("body = %q, error = %v, accepted = %q", got, err, accepted())
	}
}
