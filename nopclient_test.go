package gobblerclient

import (
	"context"
	"testing"
)

func TestNopClient_AllMethodsReturnNil(t *testing.T) {
	c := Nop()
	if err := c.Log("foo", map[string]any{"a": 1}); err != nil {
		t.Errorf("Log() on nopClient returned %v, want nil", err)
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Errorf("Flush() on nopClient returned %v, want nil", err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Errorf("Close() on nopClient returned %v, want nil", err)
	}
	if err := c.SwapServer("http://example.com"); err != nil {
		t.Errorf("SwapServer() on nopClient returned %v, want nil", err)
	}
}
