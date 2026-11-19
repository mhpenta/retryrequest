# retryrequest

A Go HTTP client that retries transient failures with jittered exponential backoff. Requires Go 1.26.11 or later.

```sh
go get github.com/mhpenta/retryrequest
```

```go
client := retryrequest.New()
body, err := client.GetBytes(ctx, "https://example.com/data")
```

`GetBytes` and `PostBytes` read the response within each attempt, so eligible requests can retry an interrupted download. Use `GetReader` to stream a response or `Do(ctx, req)` for a custom request. Close the returned reader or response body. The body helpers limit decoded output to 50 MiB by default.

## Retry policy

- **Failures.** The default policy retries transient transport errors, HTTP 408, 425, 429, and 5xx except 501, 505, and 511.
- **Retry-After.** By default, 429 and 503 honor the header. Pass `WithRetryAfterOnAllStatuses()` to use it as a minimum wait for every HTTP status your policy retries. For example, a 502 with `Retry-After: 60` ends retries immediately under the default 10-second wait cap.
- **Configurable defaults.** Up to four attempts including the first, up to 10 seconds between attempts, and a 30-second budget for scheduling retries. Set a context deadline to bound the entire request.
- **Writes.** POST and PATCH are sent once by default. To enable retries, supply an idempotency key the server honors or use `WithRetryNonIdempotent()` when repeats are harmless. Retrying a request with a body requires `req.GetBody`.

Use `errors.As` to inspect a non-2xx response as `*retryrequest.StatusError`. When retries run out, `*retryrequest.RetryError` records the attempt count and wraps the last failure.

## SEC EDGAR example

`WithRateLimiting(10, 10)` gives the client 10 round trips per second on average, with bursts of up to 10. Reuse the client across filing downloads so retries and redirects share that limit. Replace the sample User-Agent with your organization and contact email.

```go
import (
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/mhpenta/retryrequest"
)

func NewSECClient() *retryrequest.Client {
	return retryrequest.New(
		retryrequest.WithRateLimiting(10, 10),
		retryrequest.WithLongBackOffOn429(601*time.Second),
		retryrequest.WithWaitOn503(2*time.Minute),
		retryrequest.WithUserAgent("ACME Corp admin@acme.com"),
		retryrequest.WithUnlimitedAttempts(),
		retryrequest.WithMaxElapsedTime(4*time.Hour),
		retryrequest.WithBackOff(func() backoff.BackOff {
			return &backoff.ExponentialBackOff{
				InitialInterval:     15 * time.Second,
				Multiplier:          2,
				MaxInterval:         16 * time.Minute,
				RandomizationFactor: 0.1,
			}
		}),
		retryrequest.WithPerAttemptTimeout(6*time.Minute),
		retryrequest.WithMaxBodyBytes(500<<20),
		retryrequest.WithLogger(slog.Default()),
	)
}
```

[Configuration options](options.go) · [MIT license](LICENSE)
