package gobblerclient

import (
	"testing"
	"time"
)

func TestDefaultConfig_Defaults(t *testing.T) {
	c := defaultConfig()
	if c.batchSize != 100 {
		t.Errorf("batchSize = %d, want 100", c.batchSize)
	}
	if c.flushInterval != 10*time.Second {
		t.Errorf("flushInterval = %v, want 10s", c.flushInterval)
	}
	if c.types == nil {
		t.Error("types map is nil, want empty map")
	}
	if len(c.types) != 0 {
		t.Errorf("types has %d entries, want 0", len(c.types))
	}
}

func TestApplyOptions_WithTypes(t *testing.T) {
	c := applyOptions([]Option{WithTypes("alpha", "beta")})
	for _, name := range []string{"alpha", "beta"} {
		if _, ok := c.types[name]; !ok {
			t.Errorf("type %q not registered", name)
		}
	}
	if len(c.types) != 2 {
		t.Errorf("len(types) = %d, want 2", len(c.types))
	}
}

func TestApplyOptions_WithBatchSize(t *testing.T) {
	c := applyOptions([]Option{WithBatchSize(25)})
	if c.batchSize != 25 {
		t.Errorf("batchSize = %d, want 25", c.batchSize)
	}
}

func TestApplyOptions_WithFlushInterval(t *testing.T) {
	c := applyOptions([]Option{WithFlushInterval(5 * time.Second)})
	if c.flushInterval != 5*time.Second {
		t.Errorf("flushInterval = %v, want 5s", c.flushInterval)
	}
}

func TestApplyOptions_DefaultsWithNoOptions(t *testing.T) {
	c := applyOptions(nil)
	if c.batchSize != 100 {
		t.Errorf("batchSize = %d, want 100", c.batchSize)
	}
	if c.flushInterval != 10*time.Second {
		t.Errorf("flushInterval = %v, want 10s", c.flushInterval)
	}
}

func TestApplyOptions_MultipleWithTypes(t *testing.T) {
	c := applyOptions([]Option{
		WithTypes("alpha"),
		WithTypes("beta", "gamma"),
	})
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if _, ok := c.types[name]; !ok {
			t.Errorf("type %q not registered", name)
		}
	}
}

func TestDefaultConfig_MaxBufSizeDefault(t *testing.T) {
	// Default maxBufSize should be 10 × default batchSize (100) = 1000.
	c := applyOptions(nil)
	if c.maxBufSize != 1000 {
		t.Errorf("maxBufSize = %d, want 1000 (10 × batchSize)", c.maxBufSize)
	}
}

func TestDefaultConfig_MaxBufSizeFollowsBatchSize(t *testing.T) {
	// When batchSize is overridden, default maxBufSize should track it.
	c := applyOptions([]Option{WithBatchSize(50)})
	if c.maxBufSize != 500 {
		t.Errorf("maxBufSize = %d, want 500 (10 × 50)", c.maxBufSize)
	}
}

func TestDefaultConfig_MaxFlushRetries(t *testing.T) {
	c := defaultConfig()
	if c.maxFlushRetries != 3 {
		t.Errorf("maxFlushRetries = %d, want 3", c.maxFlushRetries)
	}
}

func TestApplyOptions_WithMaxBufSize(t *testing.T) {
	c := applyOptions([]Option{WithMaxBufSize(250)})
	if c.maxBufSize != 250 {
		t.Errorf("maxBufSize = %d, want 250", c.maxBufSize)
	}
}

func TestApplyOptions_WithMaxFlushRetries(t *testing.T) {
	c := applyOptions([]Option{WithMaxFlushRetries(5)})
	if c.maxFlushRetries != 5 {
		t.Errorf("maxFlushRetries = %d, want 5", c.maxFlushRetries)
	}
}
