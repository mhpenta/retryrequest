package retryrequest

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v7"
	"golang.org/x/time/rate"
)

// Option configures a Client at construction time.
type Option func(*Client)

// WithHTTPClient uses client as supplied, including its transport and redirect
// policy. A nil client selects the default.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		c.httpClient = client
		if client == nil {
			c.httpClient = defaultHTTPClient
		}
	}
}

// WithRateLimiting sets the client's sustained rate in round trips per second
// and its maximum burst. All callers, retries, and redirects through this
// client share one token bucket. Waiting counts toward the request deadline.
// A nonpositive limit or rate.Inf disables limiting. Burst values below one
// use one, which spaces round trips evenly.
func WithRateLimiting(limit rate.Limit, burst int) Option {
	return func(c *Client) {
		c.limiter = nil
		if limit > 0 && limit < rate.Inf {
			c.limiter = rate.NewLimiter(limit, max(1, burst))
		}
	}
}

// WithBackOff sets a factory that must return a fresh, non-nil backoff for
// each request. Its waits are used as given, and a Retry-After is honored as
// a minimum wait, both bounded by the elapsed-time budget and the context
// deadline; add WithMaxRetryWait to cap them. A nil factory selects the
// default jittered policy.
func WithBackOff(factory func() backoff.BackOff) Option {
	return func(c *Client) { c.newBackOff = factory }
}

// WithLongBackOffOn429 sets a minimum wait after a 429. A longer backoff or
// Retry-After takes precedence. A nonpositive duration disables this minimum.
func WithLongBackOffOn429(wait time.Duration) Option {
	return func(c *Client) { c.minWaitOn429 = max(0, wait) }
}

// WithWaitOn503 uses the server's Retry-After as the wait after a 503,
// including an explicit zero. If the header is missing or invalid, the
// client waits for the configured duration. A nonpositive duration restores
// the usual backoff. The wait cap, elapsed-time budget, and deadline apply.
func WithWaitOn503(wait time.Duration) Option {
	return func(c *Client) { c.waitOn503 = max(0, wait) }
}

// WithMaxAttempts sets the total number of attempts, including the first.
// Values below 1 mean a single attempt with no retries.
func WithMaxAttempts(n int) Option {
	return func(c *Client) { c.maxAttempts = uint(max(1, n)) }
}

// WithUnlimitedAttempts removes the attempt limit, so retries continue until
// the elapsed-time budget, the wait cap, or ctx ends them. Pair it with
// WithMaxElapsedTime, or a context deadline, to keep a request bounded.
func WithUnlimitedAttempts() Option {
	return func(c *Client) { c.maxAttempts = 0 }
}

// WithMaxElapsedTime sets the elapsed-time budget for scheduling retries. It
// includes time spent in previous attempts. A nonpositive value disables it.
func WithMaxElapsedTime(d time.Duration) Option {
	return func(c *Client) { c.maxElapsed = d }
}

// WithMaxRetryWait caps each wait between attempts, 10 seconds by default.
// Retries stop if Retry-After or a configured status delay exceeds the cap.
// A nonpositive cap allows only immediate retries. A custom WithBackOff is
// capped only when this is set.
func WithMaxRetryWait(d time.Duration) Option {
	return func(c *Client) {
		c.maxWait = max(0, d)
		c.maxWaitSet = true
	}
}

// WithRetryIf replaces DefaultRetryIf. A non-2xx response normally comes with
// a nil error, and its body holds the first 64 KiB, already read from the
// connection, which the predicate may read. A buffered read or decode failure
// comes with the 2xx response and the read error; the predicate must not
// consume or close that body.
// Cancellation, errors marked permanent, body-limit errors, invalid gzip
// headers, and requests ineligible for replay bypass the predicate. A nil
// predicate restores DefaultRetryIf.
func WithRetryIf(predicate func(*http.Response, error) bool) Option {
	return func(c *Client) {
		c.retryIf = predicate
		if predicate == nil {
			c.retryIf = DefaultRetryIf
		}
	}
}

// WithHeader appends a default header value. Headers on the request take
// precedence over defaults.
func WithHeader(key, value string) Option {
	return func(c *Client) { c.headers.Add(key, value) }
}

// WithUserAgent sets the default User-Agent.
func WithUserAgent(agent string) Option {
	return func(c *Client) { c.headers.Set("User-Agent", agent) }
}

// WithMaxBodyBytes caps output from the body helpers after any decoding.
// A nonpositive value disables the limit.
func WithMaxBodyBytes(n int64) Option {
	return func(c *Client) { c.maxBodyBytes = n }
}

// WithPerAttemptTimeout sets a timeout that covers sending the request and
// reading its body. A nonpositive value leaves timeouts to ctx and the HTTP client.
func WithPerAttemptTimeout(d time.Duration) Option {
	return func(c *Client) { c.perAttemptTimeout = d }
}

// WithLogger logs scheduled retries. A nil logger is silent.
func WithLogger(logger *slog.Logger) Option {
	return func(c *Client) { c.logger = logger }
}

// WithUserAgentFallback uses net/http's default User-Agent on
// subsequent attempts after a transport error.
func WithUserAgentFallback() Option {
	return func(c *Client) { c.fallbackUserAgent = true }
}

// WithRetryNonIdempotent allows retries of requests whose method is not
// idempotent, such as POST and PATCH. By default they are sent once, because
// a server may have processed a request whose response never arrived, and
// resending it can duplicate a write. Use it only for endpoints where a
// repeated request is harmless, such as a search or query sent as POST.
func WithRetryNonIdempotent() Option {
	return func(c *Client) { c.retryNonIdempotent = true }
}

// WithIdempotencyKeyHeader enables retries of non-idempotent requests with a
// nonblank key in header (for example "Idempotency-Key"). If the request has
// no usable key, the client generates one and sends it on every attempt.
// Use it only with servers that honour the header.
func WithIdempotencyKeyHeader(header string) Option {
	return func(c *Client) { c.idempotencyHeader = http.CanonicalHeaderKey(header) }
}

// WithBeforeAttempt calls hook before every attempt, the first included, with
// that attempt's request and its 1-based number. The request is a fresh copy
// each time, so headers the hook sets, such as a signature or a refreshed
// token, apply to that attempt only. The hook must not read req.Body; call
// req.GetBody for a copy when signing needs the payload. An error from the
// hook stops the request without sending it and is returned, joined with the
// previous attempt's failure if there was one.
func WithBeforeAttempt(hook func(req *http.Request, attempt int) error) Option {
	return func(c *Client) { c.beforeAttempt = hook }
}

// WithUTF8Conversion makes the body helpers convert text and XML responses to
// UTF-8 from the charset the response declares or its content suggests. JSON
// and ZIP media types are left as they are. Conversion rewrites the bytes, so
// leave it off for byte-exact downloads, checksums, or XML whose declaration
// names its own encoding: a decoder would convert that text a second time.
// The output limit applies after conversion. Gzip is decompressed whether or
// not this is set.
func WithUTF8Conversion() Option {
	return func(c *Client) { c.utf8Conversion = true }
}
