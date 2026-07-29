package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Shadi/trino-log-sink/internal/cache"
	"github.com/Shadi/trino-log-sink/internal/config"
)

const (
	cacheSchemaVersion = 2
	cacheKeyPrefix     = "qls"
)

// CacheObserver receives cache outcome counts. Implementations must be safe for
// concurrent use; the no-op default is used when none is supplied.
type CacheObserver interface {
	CacheHit()
	CacheMiss()
	CacheError()
}

type nopCacheObserver struct{}

func (nopCacheObserver) CacheHit()   {}
func (nopCacheObserver) CacheMiss()  {}
func (nopCacheObserver) CacheError() {}

type cachedStore struct {
	Store
	cache     cache.Cache
	listTTL   time.Duration
	queryTTL  time.Duration
	opTimeout time.Duration
	obs       CacheObserver
	log       *slog.Logger
}

// NewCached wraps a Store with a read-through cache over ListQueries and
// GetQuery. Every other method passes straight through to inner, and a cache
// miss or backend error always falls through to inner so reads never break.
func NewCached(inner Store, c cache.Cache, cfg config.Cache, obs CacheObserver, log *slog.Logger) Store {
	if obs == nil {
		obs = nopCacheObserver{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &cachedStore{
		Store:     inner,
		cache:     c,
		listTTL:   cfg.ListTTL,
		queryTTL:  cfg.QueryTTL,
		opTimeout: cfg.Timeout,
		obs:       obs,
		log:       log,
	}
}

func (s *cachedStore) ListQueries(ctx context.Context, f QueryFilter) ([]QuerySummary, error) {
	key := s.listKey(f)
	var out []QuerySummary
	if s.getCached(ctx, key, &out) {
		return out, nil
	}
	rows, err := s.Store.ListQueries(ctx, f)
	if err != nil {
		return nil, err
	}
	s.setCached(key, rows, s.listTTL)
	return rows, nil
}

func (s *cachedStore) GetQuery(ctx context.Context, queryID string) (*Row, error) {
	key := s.queryKey(queryID)
	var row Row
	if s.getCached(ctx, key, &row) {
		return &row, nil
	}
	got, err := s.Store.GetQuery(ctx, queryID)
	if err != nil {
		return nil, err
	}
	if got != nil {
		s.setCached(key, got, s.queryTTL)
	}
	return got, nil
}

func (s *cachedStore) getCached(ctx context.Context, key string, dst any) bool {
	cctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	b, err := s.cache.Get(cctx, key)
	if err != nil {
		if errors.Is(err, cache.ErrMiss) {
			s.obs.CacheMiss()
		} else {
			s.obs.CacheError()
			s.log.Debug("cache get failed", "key", key, "error", err)
		}
		return false
	}
	if err := json.Unmarshal(b, dst); err != nil {
		s.obs.CacheError()
		s.log.Debug("cache decode failed", "key", key, "error", err)
		return false
	}
	s.obs.CacheHit()
	return true
}

func (s *cachedStore) setCached(key string, val any, ttl time.Duration) {
	b, err := json.Marshal(val)
	if err != nil {
		s.log.Debug("cache encode failed", "key", key, "error", err)
		return
	}
	cctx, cancel := context.WithTimeout(context.Background(), s.opTimeout)
	defer cancel()
	if err := s.cache.Set(cctx, key, b, ttl); err != nil {
		s.obs.CacheError()
		s.log.Debug("cache set failed", "key", key, "error", err)
	}
}

func (s *cachedStore) listKey(f QueryFilter) string {
	width := f.Until.Sub(f.Since)
	bUntil := f.Until.UTC().Truncate(s.listTTL)
	bSince := bUntil.Add(-width)
	canonical := fmt.Sprintf("%d|%d|%q|%q|%q|%s|%t|%d|%d",
		bSince.Unix(), bUntil.Unix(), f.User, f.Catalog, f.State, f.Sort, f.Desc, f.Limit, f.Offset)
	sum := sha256.Sum256([]byte(canonical))
	return fmt.Sprintf("%s:v%d:list:%x", cacheKeyPrefix, cacheSchemaVersion, sum)
}

func (s *cachedStore) queryKey(queryID string) string {
	return fmt.Sprintf("%s:v%d:query:%s", cacheKeyPrefix, cacheSchemaVersion, queryID)
}
