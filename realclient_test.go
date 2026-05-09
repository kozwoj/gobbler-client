package gobblerclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ok200 returns a test server that always replies 200 with an empty ingest result.
func ok200(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ingested":0,"rejected":null}`))
	}))
}

// newDirect constructs a *realClient bypassing server validation.
// Use for tests that are not testing New() or validateServer().
func newDirect(t *testing.T, serverURL string, opts ...Option) *realClient {
	t.Helper()
	cfg := applyOptions(opts)
	rc := &realClient{
		cfg:        cfg,
		buf:        make([]bufItem, 0, cfg.batchSize),
		server:     serverURL,
		httpClient: http.DefaultClient,
		done:       make(chan struct{}),
	}
	rc.start()
	t.Cleanup(func() { _ = rc.Close(context.Background()) })
	return rc
}

func TestRealClient_New_ReturnsClient(t *testing.T) {
	srv := newValidationServer(t, true, []string{"alpha"})
	defer srv.Close()
	c, err := New(srv.URL, WithTypes("alpha"))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if c == nil {
		t.Fatal("New() returned nil client")
	}
	_ = c.Close(context.Background())
}

func TestRealClient_Log_UnknownType_ReturnsError(t *testing.T) {
	rc := newDirect(t, "http://127.0.0.1:1", WithTypes("alpha"), WithBatchSize(100))
	err := rc.Log("unknown", map[string]any{"x": 1})
	if err == nil {
		t.Fatal("Log() with unknown type returned nil, want error")
	}
}

func TestRealClient_Log_UnknownType_NoRegisteredTypes(t *testing.T) {
	rc := newDirect(t, "http://127.0.0.1:1", WithBatchSize(100)) // no types registered
	err := rc.Log("alpha", map[string]any{"x": 1})
	if err == nil {
		t.Fatal("Log() with no registered types returned nil, want error")
	}
}

func TestRealClient_Log_KnownType_BufferGrows(t *testing.T) {
	srv := ok200(t)
	defer srv.Close()
	rc := newDirect(t, srv.URL, WithTypes("alpha"), WithBatchSize(10))

	for i := 0; i < 5; i++ {
		if err := rc.Log("alpha", map[string]any{"i": i}); err != nil {
			t.Fatalf("Log() error at i=%d: %v", i, err)
		}
	}

	rc.mu.Lock()
	n := len(rc.buf)
	rc.mu.Unlock()

	if n != 5 {
		t.Errorf("buffer len = %d, want 5", n)
	}
}

func TestRealClient_Log_ThresholdTrigger_DrainBuffer(t *testing.T) {
	srv := ok200(t)
	defer srv.Close()
	const batchSize = 3
	rc := newDirect(t, srv.URL, WithTypes("alpha"), WithBatchSize(batchSize))

	// Log batchSize-1 items: buffer should hold them all.
	for i := 0; i < batchSize-1; i++ {
		if err := rc.Log("alpha", map[string]any{"i": i}); err != nil {
			t.Fatalf("Log() error at i=%d: %v", i, err)
		}
	}

	rc.mu.Lock()
	before := len(rc.buf)
	rc.mu.Unlock()

	if before != batchSize-1 {
		t.Errorf("buffer before threshold = %d, want %d", before, batchSize-1)
	}

	// The batchSize-th item triggers flush.
	if err := rc.Log("alpha", map[string]any{"i": batchSize - 1}); err != nil {
		t.Fatalf("Log() at threshold returned error: %v", err)
	}

	rc.mu.Lock()
	after := len(rc.buf)
	rc.mu.Unlock()

	if after != 0 {
		t.Errorf("buffer after threshold flush = %d, want 0", after)
	}
}

func TestRealClient_Log_MultipleTypes(t *testing.T) {
	rc := newDirect(t, "http://127.0.0.1:1", WithTypes("alpha", "beta"), WithBatchSize(100)) // no flush triggered

	if err := rc.Log("alpha", map[string]any{"a": 1}); err != nil {
		t.Errorf("Log(alpha) error: %v", err)
	}
	if err := rc.Log("beta", map[string]any{"b": 2}); err != nil {
		t.Errorf("Log(beta) error: %v", err)
	}
	if err := rc.Log("gamma", map[string]any{"g": 3}); err == nil {
		t.Error("Log(gamma) returned nil, want error for unregistered type")
	}
}

func TestRealClient_Flush_DrainBuffer(t *testing.T) {
	srv := ok200(t)
	defer srv.Close()
	rc := newDirect(t, srv.URL, WithTypes("alpha"), WithBatchSize(10))

	for i := 0; i < 4; i++ {
		_ = rc.Log("alpha", map[string]any{"i": i})
	}

	if err := rc.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error: %v", err)
	}

	rc.mu.Lock()
	n := len(rc.buf)
	rc.mu.Unlock()

	if n != 0 {
		t.Errorf("buffer after Flush() = %d, want 0", n)
	}
}

func TestRealClient_Close_DrainBuffer(t *testing.T) {
	srv := ok200(t)
	defer srv.Close()
	rc := newDirect(t, srv.URL, WithTypes("alpha"), WithBatchSize(10))

	_ = rc.Log("alpha", map[string]any{"x": 1})

	if err := rc.Close(context.Background()); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	rc.mu.Lock()
	n := len(rc.buf)
	rc.mu.Unlock()

	if n != 0 {
		t.Errorf("buffer after Close() = %d, want 0", n)
	}
}

func TestRealClient_Close_Idempotent(t *testing.T) {
	srv := ok200(t)
	defer srv.Close()
	rc := newDirect(t, srv.URL, WithTypes("alpha"))

	if err := rc.Close(context.Background()); err != nil {
		t.Fatalf("first Close() error: %v", err)
	}
	if err := rc.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error: %v", err)
	}
}

func TestRealClient_Close_ForceDrainOn5xx(t *testing.T) {
	srv := newFakeIngest(t, 500, `{"error":"server error"}`, nil)
	defer srv.Close()

	rc := newDirect(t, srv.URL, WithTypes("alpha"), WithBatchSize(100), WithMaxFlushRetries(10), WithFlushInterval(time.Hour))

	_ = rc.Log("alpha", map[string]any{"x": 1})
	err := rc.Close(context.Background())

	if err == nil {
		t.Fatal("Close() with 5xx server returned nil, want error")
	}

	rc.mu.Lock()
	n := len(rc.buf)
	rc.mu.Unlock()
	if n != 0 {
		t.Errorf("buffer after Close() with 5xx = %d, want 0 (force drained)", n)
	}
}

func TestRealClient_Ticker_FlushesAfterInterval(t *testing.T) {
	flushed := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case flushed <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ingested":1,"rejected":null}`))
	}))
	defer srv.Close()

	rc := newDirect(t, srv.URL, WithTypes("alpha"), WithBatchSize(100), WithFlushInterval(50*time.Millisecond))

	_ = rc.Log("alpha", map[string]any{"x": 1})

	select {
	case <-flushed:
		// ticker fired and flushed
	case <-time.After(2 * time.Second):
		t.Fatal("ticker did not flush within 2s")
	}
}
