package gobblerclient

import (
	"net/http"
	"time"
)

// Option is a functional option for configuring a Client.
type Option func(*config)

// config holds the resolved configuration for a realClient.
type config struct {
	types           map[string]struct{}
	batchSize       int
	flushInterval   time.Duration
	maxBufSize      int
	maxFlushRetries int
	httpClient      *http.Client
}

// WithTypes registers the type names the client should log and are understood by the Gobbler server.
// Log calls with any other type name will return an error immediately.
func WithTypes(names ...string) Option {
	return func(c *config) {
		for _, n := range names {
			c.types[n] = struct{}{}
		}
	}
}

// WithBatchSize sets the number of buffered items that triggers an automatic flush.
// Default: 100.
func WithBatchSize(n int) Option {
	return func(c *config) {
		c.batchSize = n
	}
}

// WithFlushInterval sets how often the background goroutine flushes the buffer.
// Default: 10 seconds.
func WithFlushInterval(d time.Duration) Option {
	return func(c *config) {
		c.flushInterval = d
	}
}

// WithMaxBufSize sets the maximum number of items the buffer may hold.
// Once the cap is reached, Log() drops the incoming item and returns an error
// (ErrBufferFull or ErrBufferFullServerDown) rather than appending.
// Default: 10 × batchSize (applied after all options are resolved).
func WithMaxBufSize(n int) Option {
	return func(c *config) {
		c.maxBufSize = n
	}
}

// WithMaxFlushRetries sets how many consecutive 5xx (or network) flush failures
// are tolerated before the buffer is forcibly drained to prevent unbounded growth.
// After a drain the failure counter resets and the client resumes normal operation.
// Default: 3.
func WithMaxFlushRetries(n int) Option {
	return func(c *config) {
		c.maxFlushRetries = n
	}
}

// WithHTTPClient sets the HTTP client used for all outbound requests: the
// validation GETs in New and SwapServer, and every POST /gobbler/ingest in
// Flush. Use this to configure TLS, a proxy, or a global request timeout.
// Default: an http.Client with a 15-second timeout.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *config) {
		c.httpClient = hc
	}
}

// defaultConfig returns a config with all defaults applied.
func defaultConfig() config {
	return config{
		types:           make(map[string]struct{}),
		batchSize:       100,
		flushInterval:   10 * time.Second,
		maxFlushRetries: 3,
		// maxBufSize default (10 × batchSize) is applied in applyOptions after
		// all options have been processed, so the caller's WithBatchSize is
		// respected when computing the default cap.
		maxBufSize: 0,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

// applyOptions applies the given options to the default config and returns it.
func applyOptions(opts []Option) config {
	c := defaultConfig()
	for _, o := range opts {
		o(&c)
	}
	// Apply the maxBufSize default now that batchSize is finalised.
	if c.maxBufSize == 0 {
		c.maxBufSize = 10 * c.batchSize
	}
	return c
}
