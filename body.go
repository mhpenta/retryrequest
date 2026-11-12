package retryrequest

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"golang.org/x/net/html/charset"
)

// ErrBodyTooLarge identifies a response that exceeds the helper output limit.
var ErrBodyTooLarge = errors.New("HTTP response body too large")

// BodyTooLargeError describes the limit that was exceeded.
type BodyTooLargeError struct {
	Limit int64
}

func (e *BodyTooLargeError) Error() string {
	return fmt.Sprintf("%s: limit is %d bytes", ErrBodyTooLarge, e.Limit)
}

func (e *BodyTooLargeError) Unwrap() error { return ErrBodyTooLarge }

// GetBytes fetches and reads the response body. Reading happens inside each
// attempt, so a body cut off mid-transfer is fetched again under the same
// attempt and time budget.
func (c *Client) GetBytes(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return c.readBytes(ctx, req)
}

// GetReader fetches the body as a stream. The caller must close it.
// Reading past the configured limit returns ErrBodyTooLarge. Retries end once
// the response headers arrive: a stream may already have delivered bytes, so a
// failure while reading it is returned to the caller.
func (c *Client) GetReader(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	c.prepareRequest(req)
	resp, err := c.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	return c.openBody(resp)
}

// PostBytes sends a POST request and reads the response body. The request is
// sent once unless the client was built with WithRetryNonIdempotent or
// WithIdempotencyKeyHeader.
func (c *Client) PostBytes(ctx context.Context, url, contentType string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return c.readBytes(ctx, req)
}

func (c *Client) readBytes(ctx context.Context, req *http.Request) ([]byte, error) {
	c.prepareRequest(req)
	_, payload, err := c.do(ctx, req, func(resp *http.Response) ([]byte, error) {
		body, err := c.openBody(resp)
		if err != nil {
			return nil, err
		}
		defer body.Close()
		return io.ReadAll(body)
	})
	return payload, err
}

// prepareRequest asks for gzip unless an encoding was already chosen. Asking
// here, rather than leaving it to the transport, keeps compression on for
// injected clients that disable it; openBody decompresses the response.
func (c *Client) prepareRequest(req *http.Request) {
	if req.Header.Get("Accept-Encoding") == "" && len(c.headers.Values("Accept-Encoding")) == 0 {
		req.Header.Set("Accept-Encoding", "gzip")
	}
}

// openBody decompresses gzip, converts text to UTF-8 when configured, and
// applies the output limit to a 2xx response. It closes the response body if
// it fails.
func (c *Client) openBody(resp *http.Response) (io.ReadCloser, error) {
	body, err := decodeBody(resp, c.utf8Conversion)
	if err != nil {
		return nil, err
	}
	if c.maxBodyBytes <= 0 {
		return body, nil
	}
	reader := &limitedBody{reader: body, limit: c.maxBodyBytes, remaining: c.maxBodyBytes}
	return &responseReader{Reader: reader, close: body.Close}, nil
}

func decodeBody(resp *http.Response, convertCharset bool) (io.ReadCloser, error) {
	var reader io.Reader = resp.Body
	closeBody := resp.Body.Close
	if strings.EqualFold(strings.TrimSpace(resp.Header.Get("Content-Encoding")), "gzip") {
		gzipReader, err := gzip.NewReader(reader)
		switch {
		case errors.Is(err, io.EOF):
			// An empty body labelled gzip, as some servers send for 204 or HEAD.
			reader = http.NoBody
		case err != nil:
			_ = closeBody()
			return nil, fmt.Errorf("open gzip response: %w", err)
		default:
			reader = gzipReader
			closeBody = func() error { return errors.Join(gzipReader.Close(), resp.Body.Close()) }
		}
	}

	contentType := resp.Header.Get("Content-Type")
	mediaType, _, _ := mime.ParseMediaType(contentType)
	_, subtype, _ := strings.Cut(mediaType, "/")
	isJSON := subtype == "json" || strings.HasSuffix(subtype, "+json")
	isZIP := subtype == "zip" || strings.HasSuffix(subtype, "+zip")
	isXML := subtype == "xml" || strings.HasPrefix(subtype, "xml-") || strings.HasSuffix(subtype, "+xml")
	if convertCharset && !isJSON && !isZIP && (strings.HasPrefix(mediaType, "text/") || isXML) {
		decoded, err := charset.NewReader(truncationGuard{reader}, contentType)
		switch {
		case errors.Is(err, io.EOF):
			reader = http.NoBody
		case err == nil:
			reader = decoded
		default:
			_ = closeBody()
			return nil, fmt.Errorf("decode response charset: %w", err)
		}
	}
	return &responseReader{Reader: reader, close: closeBody}, nil
}

// errTruncated is io.ErrUnexpectedEOF under another identity. charset.NewReader
// reads ahead with io.ReadFull and treats io.ErrUnexpectedEOF as a short body,
// which would silently return a truncated response as complete.
var errTruncated = fmt.Errorf("response body truncated: %w", io.ErrUnexpectedEOF)

type truncationGuard struct{ io.Reader }

func (g truncationGuard) Read(p []byte) (int, error) {
	n, err := g.Reader.Read(p)
	// Exact comparison on purpose: charset.NewReader only swallows the bare
	// sentinel, and replacing a wrapped one would discard its context.
	if err == io.ErrUnexpectedEOF { //nolint:errorlint // see above
		err = errTruncated
	}
	return n, err
}

type responseReader struct {
	io.Reader
	close func() error
}

func (r *responseReader) Close() error { return r.close() }

type limitedBody struct {
	reader    io.Reader
	limit     int64
	remaining int64
}

func (r *limitedBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining > 0 {
		n, err := r.reader.Read(p[:min(int64(len(p)), r.remaining)])
		r.remaining -= int64(n)
		return n, err
	}
	if r.remaining == 0 {
		// Distinguish a body exactly at the limit from one with more data.
		var probe [1]byte
		n, err := r.reader.Read(probe[:])
		if n == 0 {
			return 0, err
		}
		r.remaining = -1
	}
	return 0, &BodyTooLargeError{Limit: r.limit}
}
