// Package cache is a minimal, fail-open byte cache used to front slow reads.
// A miss is signalled by ErrMiss; any other error means the backend is
// unavailable and callers should fall through to the source of truth.
package cache

import (
	"context"
	"errors"
	"time"
)

// ErrMiss is returned by Get when the key is absent.
var ErrMiss = errors.New("cache: miss")

// Cache stores opaque bytes under string keys with a per-entry TTL. All methods
// must be safe for concurrent use.
type Cache interface {
	// Get returns the bytes stored for key, or ErrMiss if the key is absent.
	// Any other error means the backend is unavailable.
	Get(ctx context.Context, key string) ([]byte, error)
	// Set stores val under key for ttl on a best-effort basis.
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	// Ping verifies the backend is reachable.
	Ping(ctx context.Context) error
	Close() error
}
