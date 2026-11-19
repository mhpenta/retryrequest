package retryrequest

import (
	"bufio"
	"bytes"
	"io"
	"mime"
	"regexp"
	"strings"
	"unicode/utf8"

	"golang.org/x/net/html/charset"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/ianaindex"
)

const charsetPreviewSize = 1024

func newCharsetReader(input io.Reader, contentType string, limit int64) (io.Reader, error) {
	mediaType, params, _ := mime.ParseMediaType(contentType)
	_, subtype, _ := strings.Cut(mediaType, "/")
	isJSON := subtype == "json" || strings.HasSuffix(subtype, "+json")
	isZIP := subtype == "zip" || strings.HasSuffix(subtype, "+zip")
	isXML := subtype == "xml" || strings.HasPrefix(subtype, "xml-") || strings.HasSuffix(subtype, "+xml")
	if isJSON || isZIP || (!isXML && !strings.HasPrefix(mediaType, "text/")) {
		return input, nil
	}

	r := bufio.NewReaderSize(input, charsetPreviewSize+utf8.UTFMax-1)
	preview, err := r.Peek(charsetPreviewSize)
	if err != nil && err != io.EOF {
		return nil, err
	}

	// Only HTML uses meta tags and the HTML encoding fallback. For other
	// media types, three bytes suffice for a UTF-8 or UTF-16 BOM.
	if mediaType == "text/html" {
		enc, _, _ := charset.DetermineEncoding(preview, contentType)
		return enc.NewDecoder().Reader(r), nil
	}
	if enc, _, certain := charset.DetermineEncoding(preview[:min(3, len(preview))], ""); certain {
		return enc.NewDecoder().Reader(r), nil
	}
	label := params["charset"]
	if isXML {
		if label != "" {
			return xmlLabelReader(label, r)
		}
		return xmlCharsetReader(r, preview, limit)
	}
	if enc, _ := charset.Lookup(label); enc != nil {
		return enc.NewDecoder().Reader(r), nil
	}
	return &plainTextReader{input: r}, nil
}

// ASCII is identical in UTF-8 and Windows-1252. Stream the ASCII prefix
// while waiting for bytes that can distinguish the two encodings.
type plainTextReader struct {
	input   *bufio.Reader
	decoded io.Reader
}

func (r *plainTextReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.decoded == nil {
		if _, err := r.input.Peek(1); err != nil {
			return 0, err
		}
		buf, _ := r.input.Peek(min(len(p), r.input.Buffered()))
		n := 0
		for n < len(buf) && buf[n] < utf8.RuneSelf {
			n++
		}
		if n > 0 {
			return r.input.Read(p[:n])
		}
		decoded, err := sniffPlainText(r.input)
		if err != nil {
			return 0, err
		}
		r.decoded = decoded
	}
	return r.decoded.Read(p)
}

func sniffPlainText(r *bufio.Reader) (io.Reader, error) {
	preview, err := r.Peek(charsetPreviewSize)
	if err != nil && err != io.EOF {
		return nil, err
	}
	// Test runes starting inside the preview. Peek past its end only when
	// the final rune needs more bytes to distinguish UTF-8 from legacy text.
	for i, end := 0, len(preview); i < end; {
		for err == nil && !utf8.FullRune(preview[i:]) {
			preview, err = r.Peek(len(preview) + 1)
			if err != nil && err != io.EOF {
				return nil, err
			}
		}
		ch, size := utf8.DecodeRune(preview[i:])
		if ch == utf8.RuneError && size == 1 {
			return charmap.Windows1252.NewDecoder().Reader(r), nil
		}
		i += size
	}
	return r, nil
}

// The encoding attribute can follow the version, or appear first in an XML
// external entity's text declaration. Anchor it to exclude processing
// instructions such as xml-stylesheet and attributes in the document body.
var xmlEncoding = regexp.MustCompile(`^<\?xml(?:\s+version\s*=\s*(?:"1\.[0-9]+"|'1\.[0-9]+'))?\s+encoding\s*=\s*["']([A-Za-z][A-Za-z0-9._-]*)["']`)

func xmlCharsetReader(r *bufio.Reader, preview []byte, limit int64) (io.Reader, error) {
	switch {
	case bytes.HasPrefix(preview, []byte("\x00<\x00?")):
		return charset.NewReaderLabel("utf-16be", r)
	case bytes.HasPrefix(preview, []byte("<\x00?\x00")):
		return charset.NewReaderLabel("utf-16le", r)
	}
	attrs, ok := bytes.CutPrefix(preview, []byte("<?xml"))
	if !ok || len(attrs) == 0 || strings.IndexByte(" \t\r\n", attrs[0]) < 0 {
		return r, nil
	}
	var body io.Reader = r
	if len(preview) == charsetPreviewSize && !bytes.ContainsRune(preview, '>') {
		// A declaration may span buffers. Its ASCII bytes also count towards
		// the output limit, so stop sniffing once the caller must hit it.
		var decl bytes.Buffer
		for {
			part, err := r.ReadSlice('>')
			decl.Write(part)
			if err != nil && err != bufio.ErrBufferFull && err != io.EOF {
				return nil, err
			}
			if err != bufio.ErrBufferFull || (limit > 0 && int64(decl.Len()) > limit) {
				break
			}
		}
		preview = decl.Bytes()
		body = io.MultiReader(&decl, r)
	}
	if m := xmlEncoding.FindSubmatch(preview); m != nil {
		return xmlLabelReader(string(m[1]), body)
	}
	return body, nil // XML defaults to UTF-8.
}

func xmlLabelReader(label string, r io.Reader) (io.Reader, error) {
	// XML uses IANA charsets. For example, ISO-8859-1 and Windows-1252
	// have distinct mappings in XML. Also accept familiar HTTP aliases.
	if enc, _ := ianaindex.IANA.Encoding(label); enc != nil {
		return enc.NewDecoder().Reader(r), nil
	}
	return charset.NewReaderLabel(label, r)
}
