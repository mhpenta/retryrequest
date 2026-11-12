# retryrequest

Context-aware HTTP retries for Go 1.26.11 and later.

```sh
go get github.com/mhpenta/retryrequest
```

```go
client := retryrequest.New()
body, err := client.GetBytes(ctx, "https://example.com/data")
```

Use `GetReader(ctx, url)` for a stream and close the returned reader. Use
`PostBytes(ctx, url, contentType, payload)` for a POST with a replayable body.
For control over the request and response, build an `http.Request` and pass
it to `Do(ctx, req)`. Close the body of a successful response to release
resources, including the per-attempt timeout if configured.

`Do` returns 2xx responses and reports other statuses as `*StatusError`,
including a 3xx that reaches it, such as `304 Not Modified`; the HTTP client
follows ordinary redirects first.
It retries transport failures, 408, 425, 429, and 5xx except 501, 505, and
511. Transport failures that a retry cannot fix, such as a rejected
certificate, an unsupported URL scheme, or a header net/http refuses to send,
fail at once
(see `IsPermanentTransportError`). A request body is replayed using
`req.GetBody`. When replay is unavailable, the first failure is final.

Requests with a method that is not idempotent, such as POST and PATCH, are
sent once. When a connection drops, the client cannot tell whether the
server already applied the request, so resending it could duplicate a write.
There are three ways to make such a request retryable:

- The request carries a nonblank `Idempotency-Key` or `X-Idempotency-Key`.
- The client is built with `WithIdempotencyKeyHeader(header)` for a header the server
  honors. The client preserves a caller-supplied key and generates a key when
  needed. Every attempt sends the same key.
- The client is built with `WithRetryNonIdempotent()`, for endpoints where a
  repeat is harmless, such as a search sent as POST.

`GetBytes` and `PostBytes` read the body
inside each attempt. A body cut off mid-transfer is fetched again under the
same attempt and time budget, subject to the same method rule. On a buffered
read failure, `WithRetryIf` receives the 2xx response and the read error.
`GetReader` and `Do` stop retrying once successful headers arrive, because a
stream may already have delivered bytes to the caller.

The default policy allows four attempts with jittered exponential backoff,
a 30-second retry scheduling budget, and a 10-second cap on each wait.
On 429 and 503, a valid `Retry-After` is a floor for the next wait.
If it exceeds the wait cap or the wait would reach the caller's deadline,
the client returns the last failure with the stop reason.
`*RetryError` carries the attempt count and unwraps to the last
failure. An elapsed-time budget controls retry scheduling. Use the caller's
context or `WithPerAttemptTimeout` to bound an in-flight attempt; the default
HTTP client only times out a server that sends no response headers within
30 seconds, so long downloads are not cut off.

A custom `WithBackOff` is used as written: its waits and any `Retry-After`
are not capped unless `WithMaxRetryWait` is also set. The elapsed-time budget
and the context deadline still bound them.

## Zero values

| Option | 0 or negative means |
| --- | --- |
| `WithMaxAttempts` | one attempt, no retries (use `WithUnlimitedAttempts` for no limit) |
| `WithMaxRetryWait` | retry immediately |
| `WithMaxElapsedTime` | no time budget |
| `WithMaxBodyBytes` | no size limit |
| `WithPerAttemptTimeout` | no per-attempt timeout |

Count options take the value literally. Duration and size limits follow Go's
convention that zero means no limit, except the wait cap, where no limit
would let a `Retry-After` stall the client.

The body helpers request gzip and decompress it, including with an injected
client whose transport disables compression. A request that already sets
`Accept-Encoding`, or a client with a default `Accept-Encoding` header, keeps
that choice, and encodings other than gzip are returned as received. `Do`
behaves like `http.Client`: the default client's transport requests and
decompresses gzip transparently, and an injected client keeps its own
compression and redirect behavior.

`WithUTF8Conversion()` also converts text and XML responses to UTF-8 from the
charset they declare. JSON and ZIP are left as they are. Conversion rewrites
the bytes, so leave it off for byte-exact downloads, checksums, or XML whose
`<?xml encoding="..."?>` declaration names its own charset; an XML decoder
would otherwise convert that text a second time.

The body helpers enforce a 50 MiB output limit by default, applied after
decompression and conversion, so a gzip bomb is caught. Check it with
`errors.Is(err, retryrequest.ErrBodyTooLarge)`. Use `Do` for transport-level
response bodies and apply your own read limits. Exceeding the limit, or a body
labelled gzip that is not gzip, is returned without a retry.

## Errors

A failure that cannot be retried is returned as itself. When the client
stops retrying a failure that could have been retried, it returns a
`*RetryError` with the attempt count and a stop reason, wrapping the last
failure. Every wrapper unwraps, so `errors.As` and `errors.Is` reach the cause.

A non-2xx response becomes a `*StatusError` with the status code, host,
parsed `Retry-After`, response `Header`, and up to the first 4 KiB of the
response `Body`, decompressed if gzip-encoded. `Error()` leaves the body out,
so logging an error does not log response content.

```go
body, err := client.GetBytes(ctx, url)
var status *retryrequest.StatusError
switch {
case err == nil:
case errors.As(err, &status) && status.StatusCode == http.StatusUnprocessableEntity:
    return fmt.Errorf("rejected (request %s): %s", status.Header.Get("X-Request-Id"), status.Body)
case errors.Is(err, context.Canceled):
    return err
default:
    return fmt.Errorf("fetch failed: %w", err)
}
```

Cancellation returns the context error joined with the last failure.
`IsPermanentTransportError` and `errors.Is(err, retryrequest.ErrBodyTooLarge)`
identify the other failures that are never retried.

## Signing each attempt

`WithBeforeAttempt` runs before every attempt with a fresh copy of the
request and the attempt number, so a signature or refreshed token is set per
attempt. Read the payload through `req.GetBody`, not `req.Body`. An error
from the hook stops the request without sending it.

```go
client := retryrequest.New(retryrequest.WithBeforeAttempt(func(req *http.Request, attempt int) error {
    token, err := tokens.Current(req.Context())
    if err != nil {
        return err
    }
    req.Header.Set("Authorization", "Bearer "+token)
    return nil
}))
```

## Rate limiting

Wrap the underlying transport in your application with `golang.org/x/time/rate`.
This limits every round trip, including retries and redirects. Configure the
transport before sharing the HTTP client.

```go
import (
    "net/http"

    "github.com/mhpenta/retryrequest"
    "golang.org/x/time/rate"
)

type limitedTransport struct {
    next http.RoundTripper
    rate *rate.Limiter
}

func (t limitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
    if err := t.rate.Wait(req.Context()); err != nil {
        if req.Body != nil {
            req.Body.Close()
        }
        return nil, err
    }
    return t.next.RoundTrip(req)
}

func newRateLimitedClient() *retryrequest.Client {
    return retryrequest.New(retryrequest.WithHTTPClient(&http.Client{
        Transport: limitedTransport{next: http.DefaultTransport, rate: rate.NewLimiter(2, 1)},
    }))
}
```

## Long outages

For a request that should keep trying through a long outage, combine
`WithUnlimitedAttempts()`, `WithMaxElapsedTime(6*time.Hour)`, and
`WithBackOff(func() backoff.BackOff { return backoff.NewConstantBackOff(5*time.Minute) })`.
Use the caller's context to stop early.

## License

MIT. See [LICENSE](LICENSE).
