package retryrequest

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/text/encoding/unicode"
)

func TestBodyLimits(t *testing.T) {
	const limit = 1024
	for _, tt := range []struct {
		name        string
		contentType string
		payload     []byte
		gzip        bool
		wantLarge   bool
	}{
		{"exact limit", "application/octet-stream", bytes.Repeat([]byte("a"), limit), false, false},
		{"over limit", "application/octet-stream", bytes.Repeat([]byte("a"), limit+1), false, true},
		{"gzip expansion", "application/octet-stream", bytes.Repeat([]byte("a"), 256<<10), true, true},
		{"charset expansion", "text/plain; charset=windows-1252", bytes.Repeat([]byte{0xe9}, limit/2+1), false, true},
		{"long XML declaration", "application/xml", []byte("<?xml" + strings.Repeat(" ", 4*limit) + "version='1.0'?><p/>"), false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wire := tt.payload
			if tt.gzip {
				wire = gzipBytes(t, wire)
				if len(wire) >= limit {
					t.Fatalf("compressed fixture size = %d, want below %d", len(wire), limit)
				}
			}
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", tt.contentType)
				if tt.gzip {
					w.Header().Set("Content-Encoding", "gzip")
				}
				_, _ = w.Write(wire)
			}))
			defer server.Close()
			client := New(WithUTF8Conversion(), WithMaxBodyBytes(limit), fixedBackOff(0))

			got, err := client.GetBytes(t.Context(), server.URL)
			if tt.wantLarge {
				var tooLarge *BodyTooLargeError
				if got != nil || !errors.Is(err, ErrBodyTooLarge) || !errors.As(err, &tooLarge) || tooLarge.Limit != limit {
					t.Fatalf("received %d bytes, error = %v", len(got), err)
				}
			} else if err != nil || !bytes.Equal(got, tt.payload) {
				t.Fatalf("received %d bytes, error = %v", len(got), err)
			}
			if calls.Load() != 1 {
				t.Fatalf("attempts = %d, want 1", calls.Load())
			}

			reader, err := client.GetReader(t.Context(), server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			streamed, err := io.Copy(io.Discard, reader)
			if streamed != limit || errors.Is(err, ErrBodyTooLarge) != tt.wantLarge || !tt.wantLarge && err != nil {
				t.Fatalf("streamed %d bytes, error = %v", streamed, err)
			}
		})
	}
}

func TestUTF8Conversion(t *testing.T) {
	latin := []byte("caf\xe9")
	lateUTF8 := []byte(strings.Repeat("a", 1024) + ",東京\n")
	splitUTF8 := []byte("é" + strings.Repeat("a", 1020) + "🍵")
	longASCII := strings.Repeat("a", 1024)
	lateXML := []byte("<p>" + strings.Repeat("a", 1024) + "é</p>")
	const xmlDecl = "<?xml version='1.0' encoding = \"windows-1251\"?>"
	longDecl := "<?xml version='1.0'" + strings.Repeat(" ", 2048) + "encoding='windows-1251'?>"
	const htmlMeta = "<meta charset=windows-1251>"
	const utf16XML = "<?xml version='1.0' encoding='UTF-16BE'?><p>東京</p>"
	utf16Body, err := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewEncoder().String(utf16XML)
	if err != nil {
		t.Fatal(err)
	}
	json := []byte(`{"value":"€"}`)
	zip := []byte("PK\x03\x04\x00\x00\xff\xfe")
	for _, tt := range []struct {
		name        string
		contentType string
		payload     []byte
		want        []byte
		gzip        bool
	}{
		{"text", "Text/Plain; charset=iso-8859-1", latin, []byte("café"), false},
		{"undeclared UTF-8", "text/plain", []byte("café"), []byte("café"), false},
		{"UTF-8 after ASCII preview", "text/csv", lateUTF8, lateUTF8, true},
		{"UTF-8 split across preview", "text/plain", splitUTF8, splitUTF8, false},
		{"undeclared legacy text", "text/plain", latin, []byte("café"), false},
		{"legacy text after ASCII preview", "text/plain", []byte(longASCII + "caf\xe9"), []byte(longASCII + "café"), false},
		{"declared legacy with valid UTF-8 bytes", "text/plain; charset=windows-1252", []byte("é"), []byte("Ã©"), false},
		{"HTML meta", "text/html", []byte(htmlMeta + "\xcf"), []byte(htmlMeta + "П"), false},
		{"HTML meta in plain text", "text/plain", []byte(htmlMeta + "é"), []byte(htmlMeta + "é"), false},
		{"gzip text", "text/plain; charset=windows-1252", latin, []byte("café"), true},
		{"xml", "application/rss+xml; charset=windows-1252", latin, []byte("café"), false},
		{"XML default", "application/xml", lateXML, lateXML, false},
		{"XML ISO-8859-1", "text/xml; charset=iso-8859-1", []byte("<p>\x85</p>"), []byte("<p>\u0085</p>"), false},
		{"XML encoding declaration", "application/rss+xml", []byte(xmlDecl + "<p>\xcf</p>"), []byte(xmlDecl + "<p>П</p>"), false},
		{"long XML declaration", "text/xml", []byte(longDecl + "<p>\xcf</p>"), []byte(longDecl + "<p>П</p>"), false},
		{"XML header overrides declaration", "text/xml; charset=windows-1252", []byte(xmlDecl + "<p>\xcf</p>"), []byte(xmlDecl + "<p>Ï</p>"), false},
		{"XML BOM overrides header", "text/xml; charset=windows-1252", []byte("\ufeff<p>é</p>"), []byte("\ufeff<p>é</p>"), false},
		{"XML UTF-16BE without BOM", "application/xml", []byte(utf16Body), []byte(utf16XML), false},
		{"json", "application/problem+json; charset=windows-1252", json, json, false},
		{"zip", "application/zip; charset=windows-1252", zip, zip, false},
		{"office zip", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", zip, zip, false},
		{"binary", "application/octet-stream; charset=windows-1252", zip, zip, false},
		{"empty text", "text/plain", nil, nil, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wire := tt.payload
			if tt.gzip {
				wire = gzipBytes(t, wire)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", tt.contentType)
				if tt.gzip {
					w.Header().Set("Content-Encoding", "GZIP")
				}
				_, _ = w.Write(wire)
			}))
			defer server.Close()

			got, err := New(WithUTF8Conversion()).GetBytes(t.Context(), server.URL)
			if err != nil || !bytes.Equal(got, tt.want) {
				t.Fatalf("converted = %q, error = %v, want %q", got, err, tt.want)
			}
			// Without conversion, gzip is still decompressed but text keeps its bytes.
			got, err = New().GetBytes(t.Context(), server.URL)
			if err != nil || !bytes.Equal(got, tt.payload) {
				t.Fatalf("unconverted = %q, error = %v, want %q", got, err, tt.payload)
			}
		})
	}
}

func TestInvalidGzipClosesBodyWithoutRetry(t *testing.T) {
	body := &countedBody{Reader: strings.NewReader("invalid gzip stream")}
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Encoding": {"gzip"}}, Body: body}, nil
	})}
	_, err := New(WithHTTPClient(client), WithUTF8Conversion(), fixedBackOff(0)).GetBytes(t.Context(), "https://example.test")
	if err == nil || !body.closed || calls != 1 {
		t.Fatalf("error = %v, closed = %t, attempts = %d", err, body.closed, calls)
	}
}

func gzipBytes(t *testing.T, payload []byte) []byte {
	t.Helper()
	var wire bytes.Buffer
	w := gzip.NewWriter(&wire)
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return wire.Bytes()
}
