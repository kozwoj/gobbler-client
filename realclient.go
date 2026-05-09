package gobblerclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// bufItem holds a single buffered log entry.
type bufItem struct {
	typeName string
	fields   map[string]any
}

// realClient is the live implementation of Client.
type realClient struct {
	mu           sync.Mutex
	cfg          config
	buf          []bufItem
	server       string
	httpClient   *http.Client
	failureCount int // consecutive flush failures (network error or 5xx)
	done         chan struct{}
	closeOnce    sync.Once
}

// New constructs a Client that buffers log items and flushes them to serverURL.
// It validates the target server before returning: the pipeline must be running
// and all registered type names must be present. On failure it returns a Nop()
// client plus the validation error.
func New(serverURL string, opts ...Option) (Client, error) {
	cfg := applyOptions(opts)
	if err := validateServer(serverURL, cfg.types, cfg.httpClient); err != nil {
		return Nop(), err
	}
	rc := &realClient{
		cfg:        cfg,
		buf:        make([]bufItem, 0, cfg.batchSize),
		server:     serverURL,
		httpClient: cfg.httpClient,
		done:       make(chan struct{}),
	}
	rc.start()
	return rc, nil
}

// start launches the background goroutine that flushes on a time interval.
func (r *realClient) start() {
	ticker := time.NewTicker(r.cfg.flushInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.mu.Lock()
				_ = r.flush(context.Background())
				r.mu.Unlock()
			case <-r.done:
				return
			}
		}
	}()
}

// Log appends one item to the internal buffer.
// Returns an error immediately if typeName was not registered at construction.
// Returns ErrBufferFull or ErrBufferFullServerDown if the buffer cap is reached.
// When the buffer crosses batchSize for the first time, an automatic flush is
// triggered and its error (if any) is returned to the caller.
func (r *realClient) Log(typeName string, fields map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.cfg.types[typeName]; !ok {
		return fmt.Errorf("gobblerclient: unknown type %q", typeName)
	}

	if len(r.buf) >= r.cfg.maxBufSize {
		if r.failureCount > 0 {
			return ErrBufferFullServerDown
		}
		return ErrBufferFull
	}

	prevLen := len(r.buf)
	r.buf = append(r.buf, bufItem{typeName: typeName, fields: fields})

	if prevLen < r.cfg.batchSize && len(r.buf) >= r.cfg.batchSize {
		return r.flush(context.Background())
	}
	return nil
}

// flush sends buffered items to the server and clears the buffer.
// Must be called with r.mu held.
//
// Drain policy:
//   - Network error or 5xx → increment failureCount; hold buffer for retry.
//     If failureCount reaches maxFlushRetries, drain and reset counter.
//   - 400              → drain buffer (permanently bad payload), reset counter, return error.
//   - 200 + rejected   → drain buffer, reset counter, return error with rejection count.
//   - 200, none rejected → drain buffer, reset counter, return nil.
func (r *realClient) flush(ctx context.Context) error {
	if len(r.buf) == 0 {
		return nil
	}

	// Serialise as [{"typeName":{fields}}, ...].
	payload := make([]map[string]any, len(r.buf))
	for i, item := range r.buf {
		payload[i] = map[string]any{item.typeName: item.fields}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		// Marshal failure is a client-side bug; drain so it doesn't block forever.
		r.buf = r.buf[:0]
		return fmt.Errorf("gobblerclient: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.server+"/gobbler/ingest", bytes.NewReader(body))
	if err != nil {
		// Bad URL — drain so we don't accumulate indefinitely.
		r.buf = r.buf[:0]
		return fmt.Errorf("gobblerclient: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		// Network error → hold buffer for retry; drain after maxFlushRetries.
		r.failureCount++
		if r.failureCount >= r.cfg.maxFlushRetries {
			r.buf = r.buf[:0]
			r.failureCount = 0
		}
		return fmt.Errorf("gobblerclient: POST /gobbler/ingest: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 500 {
		// Transient server error → hold buffer for retry; drain after maxFlushRetries.
		r.failureCount++
		if r.failureCount >= r.cfg.maxFlushRetries {
			r.buf = r.buf[:0]
			r.failureCount = 0
		}
		return fmt.Errorf("gobblerclient: server error %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	// For 400 and 200: drain buffer and reset failure counter.
	r.buf = r.buf[:0]
	r.failureCount = 0

	if resp.StatusCode == http.StatusBadRequest {
		return fmt.Errorf("gobblerclient: bad request (400): %s", strings.TrimSpace(string(respBody)))
	}

	// 200: check for partial rejections.
	var result struct {
		Rejected []json.RawMessage `json:"rejected"`
	}
	if err := json.Unmarshal(respBody, &result); err == nil && len(result.Rejected) > 0 {
		return fmt.Errorf("gobblerclient: %d item(s) rejected by server", len(result.Rejected))
	}

	return nil
}

// Flush sends all buffered items to the server immediately.
func (r *realClient) Flush(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.flush(ctx)
}

// Close stops the background goroutine, flushes remaining items, and drains
// the buffer. Close is idempotent; subsequent calls return nil immediately.
func (r *realClient) Close(ctx context.Context) error {
	var err error
	r.closeOnce.Do(func() {
		close(r.done)
		r.mu.Lock()
		defer r.mu.Unlock()
		err = r.flush(ctx)
		// Always drain on Close — no further retry is possible.
		r.buf = r.buf[:0]
	})
	return err
}

// SwapServer validates newURL against the same checks as New() (pipeline
// running, all registered types present), then atomically replaces the
// endpoint. Any flush already in flight completes against the old server;
// subsequent flushes use newURL. If validation fails, the old server is kept
// and the validation error is returned.
func (r *realClient) SwapServer(newURL string) error {
	// Validate outside the lock: network I/O must not block flush callers.
	if err := validateServer(newURL, r.cfg.types, r.httpClient); err != nil {
		return err
	}
	r.mu.Lock()
	r.server = newURL
	r.mu.Unlock()
	return nil
}
