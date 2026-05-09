package gobblerclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// newValidationServer returns a test server that responds to the two validation
// endpoints used by New() and validateServer():
//   - GET /gobbler/pipeline/status → {"running": running}
//   - GET /gobbler/definition/names → names (JSON array)
//
// Any other path returns 404.
func newValidationServer(t *testing.T, running bool, names []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/gobbler/pipeline/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"running": running})
		case "/gobbler/definition/names":
			_ = json.NewEncoder(w).Encode(names)
		default:
			http.NotFound(w, r)
		}
	}))
}

// --- validateServer unit tests ---

func TestValidateServer_AllGood_NoError(t *testing.T) {
	srv := newValidationServer(t, true, []string{"alpha", "beta"})
	defer srv.Close()

	err := validateServer(srv.URL, map[string]struct{}{"alpha": {}, "beta": {}}, http.DefaultClient)
	if err != nil {
		t.Errorf("validateServer() error: %v", err)
	}
}

func TestValidateServer_NotRunning_ReturnsError(t *testing.T) {
	srv := newValidationServer(t, false, []string{"alpha"})
	defer srv.Close()

	err := validateServer(srv.URL, map[string]struct{}{"alpha": {}}, http.DefaultClient)
	if err == nil {
		t.Fatal("validateServer() returned nil for not-running server, want error")
	}
}

func TestValidateServer_MissingType_ReturnsError(t *testing.T) {
	srv := newValidationServer(t, true, []string{"alpha"})
	defer srv.Close()

	err := validateServer(srv.URL, map[string]struct{}{"alpha": {}, "beta": {}}, http.DefaultClient)
	if err == nil {
		t.Fatal("validateServer() returned nil with missing type, want error")
	}
}

func TestValidateServer_NoTypes_SkipsNamesCheck(t *testing.T) {
	// Server only has status endpoint; names endpoint would return 404.
	// With no registered types, the names check should be skipped entirely.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/gobbler/pipeline/status" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"running": true})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	err := validateServer(srv.URL, map[string]struct{}{}, http.DefaultClient)
	if err != nil {
		t.Errorf("validateServer() with no types returned error: %v", err)
	}
}

func TestValidateServer_NetworkError_ReturnsError(t *testing.T) {
	err := validateServer("http://127.0.0.1:1", map[string]struct{}{"alpha": {}}, http.DefaultClient)
	if err == nil {
		t.Fatal("validateServer() with unreachable server returned nil, want error")
	}
}

func TestValidateServer_StatusNonOK_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	err := validateServer(srv.URL, map[string]struct{}{}, http.DefaultClient)
	if err == nil {
		t.Fatal("validateServer() with 500 status returned nil, want error")
	}
}

func TestValidateServer_NamesNonOK_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/gobbler/pipeline/status":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"running": true})
		case "/gobbler/definition/names":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	err := validateServer(srv.URL, map[string]struct{}{"alpha": {}}, http.DefaultClient)
	if err == nil {
		t.Fatal("validateServer() with 500 on names returned nil, want error")
	}
}

// --- New() integration with validateServer ---

func TestNew_ValidServer_ReturnsRealClient(t *testing.T) {
	srv := newValidationServer(t, true, []string{"alpha"})
	defer srv.Close()

	c, err := New(srv.URL, WithTypes("alpha"))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer c.Close(context.Background())

	if _, ok := c.(*realClient); !ok {
		t.Errorf("New() returned %T, want *realClient", c)
	}
}

func TestNew_ServerNotRunning_ReturnsNop(t *testing.T) {
	srv := newValidationServer(t, false, []string{"alpha"})
	defer srv.Close()

	c, err := New(srv.URL, WithTypes("alpha"))
	if err == nil {
		t.Fatal("New() returned nil error for not-running server, want error")
	}
	if _, ok := c.(*nopClient); !ok {
		t.Errorf("New() returned %T on failure, want *nopClient", c)
	}
}

func TestNew_MissingType_ReturnsNop(t *testing.T) {
	srv := newValidationServer(t, true, []string{"alpha"})
	defer srv.Close()

	c, err := New(srv.URL, WithTypes("alpha", "beta"))
	if err == nil {
		t.Fatal("New() returned nil error with missing type, want error")
	}
	if _, ok := c.(*nopClient); !ok {
		t.Errorf("New() returned %T on failure, want *nopClient", c)
	}
}

func TestNew_NetworkError_ReturnsNop(t *testing.T) {
	c, err := New("http://127.0.0.1:1", WithTypes("alpha"))
	if err == nil {
		t.Fatal("New() with unreachable server returned nil error, want error")
	}
	if _, ok := c.(*nopClient); !ok {
		t.Errorf("New() returned %T on failure, want *nopClient", c)
	}
}

func TestNew_NopIsUsable_NoNilGuardNeeded(t *testing.T) {
	// Even on failure the returned client must be safe to call.
	c, _ := New("http://127.0.0.1:1", WithTypes("alpha"))
	if err := c.Log("alpha", map[string]any{"x": 1}); err != nil {
		t.Errorf("Nop.Log() returned error: %v", err)
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Errorf("Nop.Flush() returned error: %v", err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Errorf("Nop.Close() returned error: %v", err)
	}
}
