package gobblerclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// capturedRequest records the last POST body received by a fake ingest server.
type capturedRequest struct {
	body []byte
}

// newFakeIngest returns a test server that responds with statusCode and body,
// and captures the request body for inspection.
func newFakeIngest(t *testing.T, statusCode int, responseBody string, captured *capturedRequest) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if captured != nil {
			captured.body, _ = io.ReadAll(r.Body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = io.WriteString(w, responseBody)
	}))
}

// clientPointing returns a *realClient aimed at srv with batchSize=100 and the given types.
// A 1-hour flush interval prevents the background goroutine from interfering with tests.
// The client is automatically closed via t.Cleanup.
func clientPointing(t *testing.T, srv *httptest.Server, types ...string) *realClient {
	t.Helper()
	return newDirect(t, srv.URL, WithTypes(types...), WithBatchSize(100), WithFlushInterval(time.Hour))
}

// --- Serialisation ---

func TestFlush_Serialisation(t *testing.T) {
	var cap capturedRequest
	srv := newFakeIngest(t, 200, `{"ingested":2,"rejected":null}`, &cap)
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha", "beta")
	_ = rc.Log("alpha", map[string]any{"x": 1})
	_ = rc.Log("beta", map[string]any{"y": 2})

	if err := rc.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error: %v", err)
	}

	// Verify the payload is a JSON array of single-key objects.
	var payload []map[string]json.RawMessage
	if err := json.Unmarshal(cap.body, &payload); err != nil {
		t.Fatalf("captured body not valid JSON array: %v\nbody: %s", err, cap.body)
	}
	if len(payload) != 2 {
		t.Fatalf("payload len = %d, want 2", len(payload))
	}
	for _, item := range payload {
		if len(item) != 1 {
			t.Errorf("each item should have exactly 1 key, got %d", len(item))
		}
	}
}

func TestFlush_EmptyBuffer_NoRequest(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha")
	// Don't log anything — buffer is empty.
	if err := rc.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() on empty buffer returned error: %v", err)
	}
	if called {
		t.Error("server was contacted for an empty buffer, want no request")
	}
}

// --- 200 clean ---

func TestFlush_200_Clean_DrainBuffer(t *testing.T) {
	srv := newFakeIngest(t, 200, `{"ingested":1,"rejected":null}`, nil)
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha")
	_ = rc.Log("alpha", map[string]any{"a": 1})

	if err := rc.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error: %v", err)
	}

	rc.mu.Lock()
	n := len(rc.buf)
	rc.mu.Unlock()
	if n != 0 {
		t.Errorf("buffer after 200 clean = %d, want 0", n)
	}
}

func TestFlush_200_EmptyRejectedArray_NoError(t *testing.T) {
	srv := newFakeIngest(t, 200, `{"ingested":1,"rejected":[]}`, nil)
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha")
	_ = rc.Log("alpha", map[string]any{"a": 1})

	if err := rc.Flush(context.Background()); err != nil {
		t.Errorf("Flush() with empty rejected array returned error: %v", err)
	}
}

// --- 200 + rejected ---

func TestFlush_200_WithRejected_ReturnsError(t *testing.T) {
	body := `{"ingested":0,"rejected":[{"typeName":"alpha","errors":["field missing"]}]}`
	srv := newFakeIngest(t, 200, body, nil)
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha")
	_ = rc.Log("alpha", map[string]any{"a": 1})

	err := rc.Flush(context.Background())
	if err == nil {
		t.Fatal("Flush() with rejected items returned nil, want error")
	}
	if !strings.Contains(err.Error(), "rejected") {
		t.Errorf("error %q does not mention 'rejected'", err.Error())
	}
}

func TestFlush_200_WithRejected_DrainBuffer(t *testing.T) {
	body := `{"ingested":0,"rejected":[{"typeName":"alpha","errors":["bad"]}]}`
	srv := newFakeIngest(t, 200, body, nil)
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha")
	_ = rc.Log("alpha", map[string]any{"a": 1})
	_ = rc.Flush(context.Background())

	rc.mu.Lock()
	n := len(rc.buf)
	rc.mu.Unlock()
	if n != 0 {
		t.Errorf("buffer after 200+rejected = %d, want 0", n)
	}
}

// --- 400 ---

func TestFlush_400_ReturnsError(t *testing.T) {
	srv := newFakeIngest(t, 400, `{"error":"no valid items in input"}`, nil)
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha")
	_ = rc.Log("alpha", map[string]any{"a": 1})

	err := rc.Flush(context.Background())
	if err == nil {
		t.Fatal("Flush() on 400 returned nil, want error")
	}
}

func TestFlush_400_DrainBuffer(t *testing.T) {
	srv := newFakeIngest(t, 400, `{"error":"no valid items in input"}`, nil)
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha")
	_ = rc.Log("alpha", map[string]any{"a": 1})
	_ = rc.Flush(context.Background())

	rc.mu.Lock()
	n := len(rc.buf)
	rc.mu.Unlock()
	if n != 0 {
		t.Errorf("buffer after 400 = %d, want 0", n)
	}
}

// --- 5xx ---

func TestFlush_500_ReturnsError(t *testing.T) {
	srv := newFakeIngest(t, 500, `{"error":"internal server error"}`, nil)
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha")
	_ = rc.Log("alpha", map[string]any{"a": 1})

	err := rc.Flush(context.Background())
	if err == nil {
		t.Fatal("Flush() on 500 returned nil, want error")
	}
}

func TestFlush_500_HoldBuffer(t *testing.T) {
	srv := newFakeIngest(t, 500, `{"error":"internal server error"}`, nil)
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha")
	_ = rc.Log("alpha", map[string]any{"a": 1})
	_ = rc.Flush(context.Background())

	rc.mu.Lock()
	n := len(rc.buf)
	rc.mu.Unlock()
	if n != 1 {
		t.Errorf("buffer after 500 = %d, want 1 (held for retry)", n)
	}
}

func TestFlush_503_HoldBuffer(t *testing.T) {
	srv := newFakeIngest(t, 503, `{"error":"service unavailable"}`, nil)
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha")
	_ = rc.Log("alpha", map[string]any{"a": 1})
	_ = rc.Flush(context.Background())

	rc.mu.Lock()
	n := len(rc.buf)
	rc.mu.Unlock()
	if n != 1 {
		t.Errorf("buffer after 503 = %d, want 1 (held for retry)", n)
	}
}

// --- Network error ---

func TestFlush_NetworkError_HoldBuffer(t *testing.T) {
	// Point at a URL that refuses connections.
	rc := newDirect(t, "http://127.0.0.1:1", WithTypes("alpha"), WithBatchSize(100), WithMaxFlushRetries(10), WithFlushInterval(time.Hour))
	_ = rc.Log("alpha", map[string]any{"a": 1})

	err := rc.Flush(context.Background())
	if err == nil {
		t.Fatal("Flush() with no server returned nil, want error")
	}

	rc.mu.Lock()
	n := len(rc.buf)
	rc.mu.Unlock()
	if n != 1 {
		t.Errorf("buffer after network error = %d, want 1 (held for retry)", n)
	}
}

// --- Threshold-triggered flush hits real server ---

func TestLog_ThresholdFlush_HitsServer(t *testing.T) {
	var cap capturedRequest
	srv := newFakeIngest(t, 200, `{"ingested":2,"rejected":null}`, &cap)
	defer srv.Close()

	rc := clientPointing(t, srv, "alpha")
	rc.cfg.batchSize = 2

	_ = rc.Log("alpha", map[string]any{"i": 0})
	if err := rc.Log("alpha", map[string]any{"i": 1}); err != nil {
		t.Fatalf("Log() at threshold returned error: %v", err)
	}

	if len(cap.body) == 0 {
		t.Error("server never received a POST after threshold flush")
	}
}

// --- Failure counter ---

func TestFlush_FailureCounter_IncrementOn5xx(t *testing.T) {
	srv := newFakeIngest(t, 500, `{"error":"down"}`, nil)
	defer srv.Close()
	rc := clientPointing(t, srv, "alpha")
	rc.cfg.maxFlushRetries = 10

	_ = rc.Log("alpha", map[string]any{"i": 0})
	_ = rc.Flush(context.Background())

	rc.mu.Lock()
	fc := rc.failureCount
	rc.mu.Unlock()
	if fc != 1 {
		t.Errorf("failureCount = %d, want 1", fc)
	}
}

func TestFlush_FailureCounter_DrainAfterMaxRetries(t *testing.T) {
	srv := newFakeIngest(t, 500, `{"error":"down"}`, nil)
	defer srv.Close()
	rc := clientPointing(t, srv, "alpha")
	rc.cfg.maxFlushRetries = 3

	_ = rc.Log("alpha", map[string]any{"i": 0})
	_ = rc.Flush(context.Background()) // failureCount=1, buf held
	_ = rc.Flush(context.Background()) // failureCount=2, buf held
	_ = rc.Flush(context.Background()) // failureCount=3 >= maxFlushRetries, buf drained, counter reset

	rc.mu.Lock()
	n := len(rc.buf)
	fc := rc.failureCount
	rc.mu.Unlock()
	if n != 0 {
		t.Errorf("buffer after max retries = %d, want 0", n)
	}
	if fc != 0 {
		t.Errorf("failureCount after drain = %d, want 0", fc)
	}
}

func TestFlush_FailureCounter_ResetOnSuccess(t *testing.T) {
	attempt := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		if attempt < 3 {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":"down"}`))
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"ingested":1,"rejected":null}`))
		}
	}))
	defer srv.Close()
	rc := clientPointing(t, srv, "alpha")
	rc.cfg.maxFlushRetries = 10

	_ = rc.Log("alpha", map[string]any{"i": 0})
	_ = rc.Flush(context.Background()) // attempt 1 → 500, failureCount=1
	_ = rc.Flush(context.Background()) // attempt 2 → 500, failureCount=2
	_ = rc.Flush(context.Background()) // attempt 3 → 200, failureCount=0

	rc.mu.Lock()
	fc := rc.failureCount
	rc.mu.Unlock()
	if fc != 0 {
		t.Errorf("failureCount after success = %d, want 0", fc)
	}
}

// --- Buffer cap ---

func TestLog_BufferCap_ServerHealthy_ReturnsErrBufferFull(t *testing.T) {
	// batchSize=100 means no flush is triggered for 2 items; unreachable URL is never contacted.
	rc := newDirect(t, "http://127.0.0.1:1", WithTypes("alpha"), WithBatchSize(100), WithMaxBufSize(2), WithFlushInterval(time.Hour))

	_ = rc.Log("alpha", map[string]any{"i": 0})
	_ = rc.Log("alpha", map[string]any{"i": 1})

	err := rc.Log("alpha", map[string]any{"i": 2})
	if !errors.Is(err, ErrBufferFull) {
		t.Errorf("Log() at cap returned %v, want ErrBufferFull", err)
	}
}

func TestLog_BufferCap_ServerDown_ReturnsErrBufferFullServerDown(t *testing.T) {
	srv := newFakeIngest(t, 500, `{"error":"down"}`, nil)
	defer srv.Close()

	// batchSize=1 so first Log triggers a flush that fails → failureCount=1.
	rc := newDirect(t, srv.URL, WithTypes("alpha"), WithBatchSize(1), WithMaxBufSize(5), WithMaxFlushRetries(10), WithFlushInterval(time.Hour))

	// First Log: crosses threshold → flush → 500 → failureCount=1, buf held.
	_ = rc.Log("alpha", map[string]any{"i": 0})
	// Fill up to cap (buf already has 1 item; prevLen >= batchSize so no more threshold flushes).
	_ = rc.Log("alpha", map[string]any{"i": 1})
	_ = rc.Log("alpha", map[string]any{"i": 2})
	_ = rc.Log("alpha", map[string]any{"i": 3})
	_ = rc.Log("alpha", map[string]any{"i": 4})

	err := rc.Log("alpha", map[string]any{"i": 5})
	if !errors.Is(err, ErrBufferFullServerDown) {
		t.Errorf("Log() at cap with server down returned %v, want ErrBufferFullServerDown", err)
	}
}
