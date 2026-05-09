// Package gobblerclient provides a client for sending log items to a Gobbler server.
package gobblerclient

import (
	"context"
	"errors"
)

// ErrBufferFull is returned by Log when the buffer has reached its cap during
// normal operation (server healthy, log rate exceeds flush throughput).
var ErrBufferFull = errors.New("gobblerclient: buffer full, item dropped")

// ErrBufferFullServerDown is returned by Log when the buffer has reached its cap
// and at least one consecutive flush failure has occurred (server unreachable or 5xx).
var ErrBufferFullServerDown = errors.New("gobblerclient: buffer full, server unreachable")

// Client is the interface for sending log items to a Gobbler server.
// Both realClient and nopClient implement this interface.
type Client interface {
	// Log appends one item to the internal buffer. Returns an error immediately
	// if typeName was not registered at construction time, or if a threshold
	// flush triggered by this call fails.
	Log(typeName string, fields map[string]any) error

	// Flush sends all buffered items to the server now.
	Flush(ctx context.Context) error

	// Close flushes all remaining items and stops the background goroutine.
	// Close is idempotent.
	Close(ctx context.Context) error

	// SwapServer validates newURL as a Gobbler target (running + all registered
	// types present) and, if valid, atomically replaces the current endpoint.
	// On failure the current endpoint is kept unchanged.
	SwapServer(newURL string) error
}
