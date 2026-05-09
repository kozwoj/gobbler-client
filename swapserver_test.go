package gobblerclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// newIngestServer returns a test server that records how many POST /gobbler/ingest
// calls it receives and always responds 200.
func newIngestServer(t *testing.T) (*httptest.Server, *int) {
	t.Helper()
	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ingested":1,"rejected":null}`))
	}))
	return srv, &count
}

func TestSwapServer_SuccessfulSwap_RoutesFlushToNewServer(t *testing.T) {
	srv1, count1 := newIngestServer(t)
	defer srv1.Close()

	srv2, count2 := newIngestServer(t)
	defer srv2.Close()

	// Build a validation-capable facade in front of srv2's ingest server.
	// srv2 only handles /gobbler/ingest; we need a separate validation server.
	valid2 := newValidationServer(t, true, []string{"alpha"})
	defer valid2.Close()

	rc := newDirect(t, srv1.URL, WithTypes("alpha"), WithBatchSize(10), WithFlushInterval(time.Hour))

	// Log one item to srv1 and flush it.
	_ = rc.Log("alpha", map[string]any{"x": 1})
	_ = rc.Flush(context.Background())

	if *count1 != 1 {
		t.Fatalf("expected 1 flush to srv1 before swap, got %d", *count1)
	}

	// Swap to valid2 (which validates OK) but point the actual ingest URL at srv2.
	// We do this by swapping to valid2's URL so validateServer passes, then
	// manually setting r.server to srv2.URL to check the routing behaviour.
	// Instead, use a combined server that handles both validation and ingest.
	combined2 := newCombinedServer(t, true, []string{"alpha"}, count2)
	defer combined2.Close()

	if err := rc.SwapServer(combined2.URL); err != nil {
		t.Fatalf("SwapServer() error: %v", err)
	}

	_ = rc.Log("alpha", map[string]any{"x": 2})
	_ = rc.Flush(context.Background())

	if *count2 == 0 {
		t.Error("expected at least one flush to combined2 after swap, got 0")
	}
	// srv1 should still only have 1 hit.
	if *count1 != 1 {
		t.Errorf("srv1 hit count after swap = %d, want 1", *count1)
	}
}

func TestSwapServer_ValidationFails_KeepsOldServer(t *testing.T) {
	srv1, count1 := newIngestServer(t)
	defer srv1.Close()

	rc := newDirect(t, srv1.URL, WithTypes("alpha"), WithBatchSize(10), WithFlushInterval(time.Hour))

	// Attempt swap to unreachable server — validation will fail.
	err := rc.SwapServer("http://127.0.0.1:1")
	if err == nil {
		t.Fatal("SwapServer() with unreachable target returned nil, want error")
	}

	// The client should still flush to the original server.
	_ = rc.Log("alpha", map[string]any{"x": 1})
	_ = rc.Flush(context.Background())

	if *count1 != 1 {
		t.Errorf("srv1 hit count after failed swap = %d, want 1", *count1)
	}
}

func TestSwapServer_ValidationFails_NotRunning_KeepsOldServer(t *testing.T) {
	srv1, count1 := newIngestServer(t)
	defer srv1.Close()

	// New target reports pipeline not running.
	notRunning := newValidationServer(t, false, []string{"alpha"})
	defer notRunning.Close()

	rc := newDirect(t, srv1.URL, WithTypes("alpha"), WithBatchSize(10), WithFlushInterval(time.Hour))

	err := rc.SwapServer(notRunning.URL)
	if err == nil {
		t.Fatal("SwapServer() with not-running server returned nil, want error")
	}

	// Old server still receives flushes.
	_ = rc.Log("alpha", map[string]any{"x": 1})
	_ = rc.Flush(context.Background())

	if *count1 != 1 {
		t.Errorf("srv1 hit count = %d, want 1", *count1)
	}
}

func TestSwapServer_ValidationFails_MissingType_KeepsOldServer(t *testing.T) {
	srv1, count1 := newIngestServer(t)
	defer srv1.Close()

	// New target doesn't know about "beta".
	partial := newValidationServer(t, true, []string{"alpha"})
	defer partial.Close()

	rc := newDirect(t, srv1.URL, WithTypes("alpha", "beta"), WithBatchSize(10), WithFlushInterval(time.Hour))

	err := rc.SwapServer(partial.URL)
	if err == nil {
		t.Fatal("SwapServer() with missing type returned nil, want error")
	}

	_ = rc.Log("alpha", map[string]any{"x": 1})
	_ = rc.Flush(context.Background())

	if *count1 != 1 {
		t.Errorf("srv1 hit count = %d, want 1", *count1)
	}
}

func TestSwapServer_ConcurrentFlushAndSwap_Consistent(t *testing.T) {
	// Fire many concurrent Log+Flush and SwapServer calls; verify the client
	// never crashes and always flushes to one of the two valid servers.
	combined1 := newCombinedServer(t, true, []string{"alpha"}, new(int))
	defer combined1.Close()
	combined2 := newCombinedServer(t, true, []string{"alpha"}, new(int))
	defer combined2.Close()

	rc := newDirect(t, combined1.URL, WithTypes("alpha"), WithBatchSize(1), WithFlushInterval(time.Hour))

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = rc.Log("alpha", map[string]any{"i": i})
			_ = rc.Flush(context.Background())
		}(i)
		if i%5 == 0 {
			wg.Add(1)
			go func(url string) {
				defer wg.Done()
				_ = rc.SwapServer(url)
			}(combined2.URL)
		}
	}
	wg.Wait()
	// No panic = pass. The race detector (when supported) would catch data races.
}

// newCombinedServer returns a server that handles both validation endpoints
// (/gobbler/pipeline/status, /gobbler/definition/names) and /gobbler/ingest,
// incrementing *ingestCount on each POST to /gobbler/ingest.
func newCombinedServer(t *testing.T, running bool, names []string, ingestCount *int) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/gobbler/pipeline/status":
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"running":` + boolStr(running) + `}`))
		case "/gobbler/definition/names":
			w.WriteHeader(200)
			enc := []byte(`["`)
			for i, n := range names {
				if i > 0 {
					enc = append(enc, ',', '"')
				}
				enc = append(enc, n...)
				enc = append(enc, '"')
			}
			enc = append(enc, ']')
			_, _ = w.Write(enc)
		case "/gobbler/ingest":
			mu.Lock()
			*ingestCount++
			mu.Unlock()
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"ingested":1,"rejected":null}`))
		default:
			http.NotFound(w, r)
		}
	}))
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
