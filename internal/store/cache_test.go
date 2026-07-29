package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shadi/trino-log-sink/internal/cache"
	"github.com/Shadi/trino-log-sink/internal/config"
)

type countingStore struct {
	mu         sync.Mutex
	listCalls  int
	getCalls   int
	listResult []QuerySummary
	getResult  *Row
	listErr    error
	getErr     error
}

func (c *countingStore) Validate(context.Context) error            { return nil }
func (c *countingStore) InsertBatch(context.Context, []Row) error  { return nil }
func (c *countingStore) Prune(context.Context, time.Time) error    { return nil }
func (c *countingStore) Maintain(context.Context, string) error    { return nil }
func (c *countingStore) Optimize(context.Context, time.Time) error { return nil }
func (c *countingStore) Close() error                              { return nil }

func (c *countingStore) ListQueries(context.Context, QueryFilter) ([]QuerySummary, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listCalls++
	return c.listResult, c.listErr
}

func (c *countingStore) GetQuery(context.Context, string) (*Row, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getCalls++
	return c.getResult, c.getErr
}

type fakeCache struct {
	mu       sync.Mutex
	data     map[string][]byte
	failGet  bool
	failSet  bool
	setCalls int
}

func newFakeCache() *fakeCache { return &fakeCache{data: map[string][]byte{}} }

func (f *fakeCache) Get(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failGet {
		return nil, errors.New("backend down")
	}
	b, ok := f.data[key]
	if !ok {
		return nil, cache.ErrMiss
	}
	return b, nil
}

func (f *fakeCache) Set(_ context.Context, key string, val []byte, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls++
	if f.failSet {
		return errors.New("backend down")
	}
	f.data[key] = val
	return nil
}

func (f *fakeCache) Ping(context.Context) error { return nil }
func (f *fakeCache) Close() error               { return nil }

type countingObs struct{ hits, misses, errs atomic.Int64 }

func (o *countingObs) CacheHit()   { o.hits.Add(1) }
func (o *countingObs) CacheMiss()  { o.misses.Add(1) }
func (o *countingObs) CacheError() { o.errs.Add(1) }

var testCacheCfg = config.Cache{
	Addr:     "fake",
	ListTTL:  10 * time.Minute,
	QueryTTL: time.Hour,
	Timeout:  time.Second,
}

var bucketBase = time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)

func listFilter(until time.Time) QueryFilter {
	return QueryFilter{
		Since: until.Add(-time.Hour), Until: until,
		Sort: SortStart, Desc: true, Limit: 101,
	}
}

func TestCachedListMissThenHit(t *testing.T) {
	inner := &countingStore{listResult: []QuerySummary{{QueryID: "a"}}}
	fc := newFakeCache()
	obs := &countingObs{}
	s := NewCached(inner, fc, testCacheCfg, obs, nil)
	ctx := context.Background()
	f := listFilter(bucketBase.Add(time.Minute))

	got, err := s.ListQueries(ctx, f)
	if err != nil || len(got) != 1 {
		t.Fatalf("first list: got %v err %v", got, err)
	}
	if inner.listCalls != 1 || obs.misses.Load() != 1 || fc.setCalls != 1 {
		t.Fatalf("after miss: listCalls=%d misses=%d setCalls=%d", inner.listCalls, obs.misses.Load(), fc.setCalls)
	}

	got, err = s.ListQueries(ctx, f)
	if err != nil || len(got) != 1 {
		t.Fatalf("second list: got %v err %v", got, err)
	}
	if inner.listCalls != 1 || obs.hits.Load() != 1 {
		t.Fatalf("after hit: listCalls=%d hits=%d", inner.listCalls, obs.hits.Load())
	}
}

func TestCachedListBucketStability(t *testing.T) {
	inner := &countingStore{listResult: []QuerySummary{{QueryID: "a"}}}
	s := NewCached(inner, newFakeCache(), testCacheCfg, &countingObs{}, nil)
	ctx := context.Background()

	if _, err := s.ListQueries(ctx, listFilter(bucketBase.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListQueries(ctx, listFilter(bucketBase.Add(5*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if inner.listCalls != 1 {
		t.Fatalf("same bucket should hit: listCalls=%d", inner.listCalls)
	}

	if _, err := s.ListQueries(ctx, listFilter(bucketBase.Add(12*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if inner.listCalls != 2 {
		t.Fatalf("next bucket should miss: listCalls=%d", inner.listCalls)
	}
}

func TestCachedGetQueryNilNotCached(t *testing.T) {
	inner := &countingStore{getResult: nil}
	s := NewCached(inner, newFakeCache(), testCacheCfg, &countingObs{}, nil)
	ctx := context.Background()

	for range 2 {
		got, err := s.GetQuery(ctx, "missing")
		if err != nil || got != nil {
			t.Fatalf("got %v err %v", got, err)
		}
	}
	if inner.getCalls != 2 {
		t.Fatalf("nil result must not be cached: getCalls=%d", inner.getCalls)
	}
}

func TestCachedGetQueryHit(t *testing.T) {
	inner := &countingStore{getResult: &Row{QueryID: "x"}}
	s := NewCached(inner, newFakeCache(), testCacheCfg, &countingObs{}, nil)
	ctx := context.Background()

	if _, err := s.GetQuery(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetQuery(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	if inner.getCalls != 1 {
		t.Fatalf("non-nil row should be cached: getCalls=%d", inner.getCalls)
	}
}

func TestCachedFailOpen(t *testing.T) {
	inner := &countingStore{listResult: []QuerySummary{{QueryID: "a"}}}
	fc := newFakeCache()
	fc.failGet = true
	fc.failSet = true
	obs := &countingObs{}
	s := NewCached(inner, fc, testCacheCfg, obs, nil)

	got, err := s.ListQueries(context.Background(), listFilter(bucketBase))
	if err != nil || len(got) != 1 {
		t.Fatalf("fail-open list: got %v err %v", got, err)
	}
	if inner.listCalls != 1 {
		t.Fatalf("must fall through to inner: listCalls=%d", inner.listCalls)
	}
	if obs.errs.Load() != 2 {
		t.Fatalf("get and set errors should be counted: errs=%d", obs.errs.Load())
	}
}

func TestCachedStoreErrorNotMasked(t *testing.T) {
	inner := &countingStore{listErr: errors.New("trino down")}
	fc := newFakeCache()
	s := NewCached(inner, fc, testCacheCfg, &countingObs{}, nil)

	if _, err := s.ListQueries(context.Background(), listFilter(bucketBase)); err == nil {
		t.Fatal("store error must propagate")
	}
	if fc.setCalls != 0 {
		t.Fatalf("must not cache on store error: setCalls=%d", fc.setCalls)
	}
}

func TestCachedGetQueryRoundTrip(t *testing.T) {
	est := time.Date(2026, 6, 30, 11, 0, 0, 0, time.UTC)
	want := &Row{
		QueryID:            "x",
		CreateTime:         bucketBase,
		ExecutionStartTime: &est,
		EndTime:            nil,
		WallMS:             1 << 60,
	}
	inner := &countingStore{getResult: want}
	s := NewCached(inner, newFakeCache(), testCacheCfg, &countingObs{}, nil)
	ctx := context.Background()

	if _, err := s.GetQuery(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetQuery(ctx, "x")
	if err != nil || got == nil {
		t.Fatalf("got %v err %v", got, err)
	}
	if got.QueryID != want.QueryID || got.WallMS != want.WallMS {
		t.Fatalf("scalar mismatch: %+v", got)
	}
	if !got.CreateTime.Equal(want.CreateTime) {
		t.Fatalf("create_time mismatch: %v vs %v", got.CreateTime, want.CreateTime)
	}
	if got.ExecutionStartTime == nil || !got.ExecutionStartTime.Equal(est) {
		t.Fatalf("execution_start_time mismatch: %v", got.ExecutionStartTime)
	}
	if got.EndTime != nil {
		t.Fatalf("end_time should stay nil: %v", got.EndTime)
	}
}
