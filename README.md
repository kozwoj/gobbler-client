# gobbler-client

`gobbler-client` is a Go SDK for buffering and sending structured log items to a [Gobbler](https://github.com/kozwoj/gobbler) telemetry ingestion server.

The client accumulates sent items in an in-memory buffer, and flushes them in batches — either when the batch threshold is reached or on a background timer. The design is fire-and-forget: `Log()` never blocks waiting for network I/O. A `Nop()` client is available for the "logging disabled" state and for testing, so application using the SDK need no `if loggingEnabled` guards.

---

## Installation

```
go get github.com/kozwoj/gobbler-client@latest
```

Requires Go 1.24 or later.

---

## Prerequisites

A running Gobbler (logger) instance with the item type definitions you intend to log already registered via `POST /gobbler/definition/add`. `New()` validates the target before returning — it will fail if the pipeline is not running or if any registered type name is absent.

See the [Gobbler REST reference](https://github.com/kozwoj/gobbler/blob/main/docs/REST-commands.md) for how to start a Gobbler instance and register types.

---

## Quick start

```go
import gobblerclient "github.com/kozwoj/gobbler-client"

// Create a client. New() validates the target server before returning.
client, err := gobblerclient.New(
    "http://my-gobbler-server:8080",
    gobblerclient.WithTypes("my-event"),
    gobblerclient.WithBatchSize(50),
    gobblerclient.WithFlushInterval(10*time.Second),
)
if err != nil {
    // Target unreachable or type not registered.
    // Fall back to Nop so the rest of the application is unaffected.
    client = gobblerclient.Nop()
}

// Log an item. Returns immediately; the item is buffered, not sent.
_ = client.Log("my-event", map[string]any{
    "userId":     "u-123",
    "durationMs": 42,
})

// On shutdown: flush remaining items and stop the background goroutine.
_ = client.Close(context.Background())
```

In practice, options are often resolved from configuration at runtime. The Gobbler server
builds its own client this way (`server/pipeline_routes.go` — `handlePipelineStart`):

```go
opts := []gobblerclient.Option{
    gobblerclient.WithTypes(s.config.LoggerTypes...),
}
if s.config.LoggerBatchSize > 0 {
    opts = append(opts, gobblerclient.WithBatchSize(s.config.LoggerBatchSize))
}
if s.config.LoggerFlushInterval != "" {
    if d, err := time.ParseDuration(s.config.LoggerFlushInterval); err == nil {
        opts = append(opts, gobblerclient.WithFlushInterval(d))
    }
}
client, err := gobblerclient.New(s.config.LoggerEndpoint, opts...)
if err != nil {
    s.logger = gobblerclient.Nop() // pipeline still starts
} else {
    s.logger = client
}
```

---

## The Nop client

`Nop()` returns a no-op `Client` where every method returns `nil` immediately. Use it when logging is disabled, when `New()` fails, or in unit tests that don't care about log output.

The recommended pattern is to initialise any `Client` field to `Nop()` at construction time and replace it with a real client when the configuration arrives. This means every `Log()` call site is unconditional — no `if loggingEnabled` guards needed anywhere:

```go
type MyService struct {
    logger gobblerclient.Client // never nil
}

func NewMyService() *MyService {
    return &MyService{logger: gobblerclient.Nop()}
}

func (s *MyService) ConfigureLogging(endpoint string, types []string) {
    c, err := gobblerclient.New(endpoint, gobblerclient.WithTypes(types...))
    if err != nil {
        return // keep Nop
    }
    s.logger = c
}
```

The Gobbler server uses exactly this pattern (`server/server.go` — `New()`):

```go
return &Server{
    ...
    logger: gobblerclient.Nop(), // replaced with a real client on pipeline start
}
```

---

## Options reference

| Option                  | Default           | Description |
|-------------------------|-------------------|-------------|
| `WithTypes(names...)`   | *(none)*          | Register the type names this client may log. `Log()` returns an error immediately for any other name. |
| `WithBatchSize(n)`      | `100`             | Number of buffered items that triggers an automatic flush. |
| `WithFlushInterval(d)`  | `10s`             | How often the background goroutine flushes the buffer. |
| `WithMaxBufSize(n)`     | `10 × batchSize`  | Maximum number of items the buffer may hold. Items beyond this cap are dropped and an error is returned. |
| `WithMaxFlushRetries(n)`| `3`               | Consecutive flush failures (network error or 5xx) before the buffer is forcibly drained to prevent unbounded growth. The failure counter resets after a drain. |
| `WithHTTPClient(hc)`    | 15 s timeout      | Custom `*http.Client` for all outbound requests. Use this to configure TLS, a proxy, or a global timeout. |

---

## Client interface

```go
type Client interface {
    Log(typeName string, fields map[string]any) error
    Flush(ctx context.Context) error
    Close(ctx context.Context) error
    SwapServer(newURL string) error
}
```

| Method          | Description |
|-----------------|-------------|
| `Log`           | Buffer one item. Returns an error if `typeName` was not registered at construction, or if the buffer is full. Never blocks on network I/O. |
| `Flush`         | Send all buffered items to the server immediately. Respects the provided context for cancellation. |
| `Close`         | Flush remaining items, stop the background goroutine, and release resources. Idempotent — safe to call more than once. |
| `SwapServer`    | Validate `newURL` as a Gobbler target (pipeline running + all registered types present) and atomically replace the current endpoint. The current endpoint is kept unchanged on failure. |

### Testing

In unit tests that don't need real log delivery, pass `gobblerclient.Nop()` wherever a `Client` is required. For tests that assert on logged items, implement the `Client` interface with a spy struct — the interface is small (four methods) and straightforward to stub.

---

## Real example: Gobbler monitoring itself

Gobbler uses gobbler-client to emit its own operational telemetry to a second Gobbler instance — "Gobbler monitoring Gobbler". This is a complete, production-quality example of every major SDK pattern.

### What gets logged

Four item types are emitted by the instrumented Gobbler instance and stored by the logger instance:

| Type | Emitted by | One record per |
|---|---|---|
| `gobbler-ingest-event` | ingest handler | `POST /gobbler/ingest` request |
| `gobbler-writer-flush` | `FileWriter` / `BlobWriter` | successful flush to file or blob |
| `gobbler-writer-error` | `FileWriter` / `BlobWriter` | write, rotate, or open failure |
| `gobbler-pipeline-event` | pipeline start/stop/rotate handlers | pipeline lifecycle event |

### Setting up the logger Gobbler instance

Before the instrumented Gobbler can send telemetry, the four item type definitions above must be registered on the logger instance. The `scripts/setup-logger.ps1` script in the [Gobbler server repository](https://github.com/kozwoj/gobbler) does this in three steps:

1. Registers each of the four item type definitions via `POST /gobbler/definition/add`.
2. Configures the logger instance in file mode (`POST /gobbler/pipeline/configure`).
3. Starts its pipeline (`POST /gobbler/pipeline/start`).

Assume the logger Gobbler runs on `logs.internal:9200`. Start it first, then run the setup script pointing at that address:

```powershell
# In the gobbler repository:
.\scripts\setup-logger.ps1 -LoggerUrl http://logs.internal:9200 -OutputDir /var/gobbler-logs
```

The logger instance is now ready to receive telemetry.

### Configuring the client in the instrumented instance

Self-logging is enabled by adding logger fields to the `POST /gobbler/pipeline/configure` call on the instrumented Gobbler. The `loggerEndpoint` must point at the logger instance set up in the previous step:

```json
{
  "mode": "blob",
  "accountName": "...",
  "accountKey": "...",
  "writerQueueSize": 100,
  "writerBatchSize": 50,
  "loggerEndpoint": "http://logs.internal:9200",
  "loggerTypes": [
    "gobbler-ingest-event",
    "gobbler-writer-flush",
    "gobbler-writer-error",
    "gobbler-pipeline-event"
  ],
  "loggerBatchSize": 20,
  "loggerFlushInterval": "10s"
}
```

When `loggerEndpoint` is omitted or empty, that disables self-logging; a `Nop()` client is used and nothing is sent.

### Initiating the client

The client is created at pipeline start (`server/pipeline_routes.go` — `handlePipelineStart`). Options come directly from the configure payload. The pipeline always starts even if client creation fails — a soft failure that keeps the instrumented instance operational:

```go
opts := []gobblerclient.Option{
    gobblerclient.WithTypes(s.config.LoggerTypes...),
}
if s.config.LoggerBatchSize > 0 {
    opts = append(opts, gobblerclient.WithBatchSize(s.config.LoggerBatchSize))
}
if s.config.LoggerFlushInterval != "" {
    if d, err := time.ParseDuration(s.config.LoggerFlushInterval); err == nil {
        opts = append(opts, gobblerclient.WithFlushInterval(d))
    }
}
client, err := gobblerclient.New(s.config.LoggerEndpoint, opts...)
if err != nil {
    s.loggerErr = err.Error()
    s.logger = gobblerclient.Nop() // pipeline still starts
} else {
    s.logger = client
}
_ = s.logger.Log("gobbler-pipeline-event", map[string]any{"event": "start"})
```

`New()` validates the logger instance before returning: it checks that the pipeline is running and that all four type names are registered with the logger instance. If the logger is down or not yet set up, `New()` fails and the `Nop()` fallback silences all subsequent `Log()` calls.

### Sending log items

**Per ingest request** — `server/ingest_routes.go`, `logIngestEvent()`.

The client field is protected by a server mutex, so a snapshot is taken under a read-lock before calling `Log()`. This avoids holding the lock across the (non-blocking) buffer append:

```go
s.mu.RLock()
logger := s.logger
s.mu.RUnlock()
_ = logger.Log("gobbler-ingest-event", map[string]any{
    "requestId":  middleware.GetReqID(r.Context()),
    "itemsIn":    itemsIn,
    "ingested":   ingested,
    "rejected":   rejected,
    "statusCode": statusCode,
    "durationMs": time.Since(start).Milliseconds(),
})
```

**Per writer flush and error** — `writers/file_writer.go` and `writers/blob_writer.go`, `flush()`.

Each writer holds its own `Client` field (injected via `SetLogger` at pipeline start). Both success and error outcomes are logged with a consistent field set:

```go
// on error
_ = w.logger.Log("gobbler-writer-error", map[string]any{
    "itemType":  w.typeName,
    "operation": "write-file",
    "errorMsg":  err.Error(),
})

// on success
_ = w.logger.Log("gobbler-writer-flush", map[string]any{
    "itemType":     w.typeName,
    "itemsFlushed": itemsCount,
    "output":       w.file.Name(),
})
```

**Pipeline lifecycle** — `server/pipeline_routes.go`.

Start and rotate are logged immediately after the action. Stop is logged last, before `Close()`, to ensure the event is included in the final flush:

```go
// on start
_ = s.logger.Log("gobbler-pipeline-event", map[string]any{"event": "start"})

// on rotate
_ = s.logger.Log("gobbler-pipeline-event", map[string]any{"event": "rotate", "itemType": req.TypeName})

// on stop — last Log() before Close()
_ = s.logger.Log("gobbler-pipeline-event", map[string]any{"event": "stop"})
_ = s.logger.Close(r.Context()) // flushes everything and stops the background goroutine
s.logger = gobblerclient.Nop()  // safe fallback for any stray calls after shutdown
```

---

## Error handling

| Error                       | When returned |
|-----------------------------|---------------|
| `ErrBufferFull`             | Buffer is at capacity; server appears healthy (no recent flush failures). Item was dropped. |
| `ErrBufferFullServerDown`   | Buffer is at capacity and at least one consecutive flush failure has occurred. Item was dropped. |
| `New()` / `SwapServer()` validation error | Pipeline not running at the target URL, or a registered type name is missing from the target server. |

Errors from `Log()` are informational — the client continues operating normally after a drop. The recommended practice is to ignore them at individual call sites (`_ = client.Log(...)`) and monitor the Gobbler server's own ingest metrics for signs of back-pressure.

---

## Versioning

This module follows semantic versioning. See the [releases page](https://github.com/kozwoj/gobbler-client/releases) for the changelog.
