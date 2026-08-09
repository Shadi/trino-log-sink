package ingest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shadi/trino-log-sink/internal/store"
)

type fakeWriter struct {
	mu        sync.Mutex
	batches   [][]store.Row
	failTimes int
	err       error
	calls     atomic.Int64
	started   chan struct{}
	block     chan struct{}
}

func (f *fakeWriter) InsertBatch(_ context.Context, rows []store.Row) error {
	f.calls.Add(1)
	if f.started != nil {
		f.started <- struct{}{}
	}
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failTimes > 0 {
		f.failTimes--
		if f.err != nil {
			return f.err
		}
		return errors.New("boom")
	}
	f.batches = append(f.batches, append([]store.Row(nil), rows...))
	return nil
}

func (f *fakeWriter) snapshot() [][]store.Row {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]store.Row(nil), f.batches...)
}

type fakeObs struct{ enq, drop, flushed, failed atomic.Int64 }

func (o *fakeObs) Enqueued()     { o.enq.Add(1) }
func (o *fakeObs) Dropped(n int) { o.drop.Add(int64(n)) }
func (o *fakeObs) Flushed(n int) { o.flushed.Add(int64(n)) }
func (o *fakeObs) FlushFailed()  { o.failed.Add(1) }

func row(id string) store.Row { return store.Row{QueryID: id} }

func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestFlushOnBatchSize(t *testing.T) {
	w := &fakeWriter{}
	b := New(w, Config{BatchSize: 3, FlushInterval: time.Hour, BufferCapacity: 10}, nil, nil)
	defer b.Close(context.Background())

	b.Add(row("a"))
	b.Add(row("b"))
	b.Add(row("c"))

	eventually(t, time.Second, func() bool {
		bs := w.snapshot()
		return len(bs) == 1 && len(bs[0]) == 3
	})
}

func TestFlushOnInterval(t *testing.T) {
	w := &fakeWriter{}
	b := New(w, Config{BatchSize: 1000, FlushInterval: 15 * time.Millisecond, BufferCapacity: 10}, nil, nil)
	defer b.Close(context.Background())

	b.Add(row("a"))
	b.Add(row("b"))

	eventually(t, time.Second, func() bool {
		bs := w.snapshot()
		return len(bs) >= 1 && len(bs[0]) == 2
	})
}

func TestDedupeWithinBatch(t *testing.T) {
	w := &fakeWriter{}
	b := New(w, Config{BatchSize: 3, FlushInterval: time.Hour, BufferCapacity: 10}, nil, nil)
	defer b.Close(context.Background())

	b.Add(row("a"))
	b.Add(row("b"))
	b.Add(row("a"))

	eventually(t, time.Second, func() bool {
		bs := w.snapshot()
		return len(bs) == 1 && len(bs[0]) == 2
	})
}

func TestGracefulCloseDrains(t *testing.T) {
	w := &fakeWriter{}
	b := New(w, Config{BatchSize: 1000, FlushInterval: time.Hour, BufferCapacity: 10}, nil, nil)

	b.Add(row("a"))
	b.Add(row("b"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	bs := w.snapshot()
	if len(bs) != 1 || len(bs[0]) != 2 {
		t.Fatalf("expected one drained batch of 2, got %v", bs)
	}
}

func TestRetryThenSucceed(t *testing.T) {
	w := &fakeWriter{failTimes: 2}
	obs := &fakeObs{}
	b := New(w, Config{BatchSize: 1, FlushInterval: time.Hour, BufferCapacity: 10, MaxRetries: 3}, nil, obs)
	defer b.Close(context.Background())

	b.Add(row("a"))

	eventually(t, 3*time.Second, func() bool {
		return len(w.snapshot()) == 1 && obs.flushed.Load() == 1
	})
	if obs.failed.Load() != 0 {
		t.Errorf("should not have recorded a flush failure")
	}
}

func TestDropCountOnExhaustedRetries(t *testing.T) {
	w := &fakeWriter{failTimes: 10}
	obs := &fakeObs{}
	b := New(w, Config{BatchSize: 2, FlushInterval: time.Hour, BufferCapacity: 10, MaxRetries: 1}, nil, obs)
	defer b.Close(context.Background())

	b.Add(row("a"))
	b.Add(row("b"))

	eventually(t, 3*time.Second, func() bool {
		return obs.failed.Load() == 1 && obs.drop.Load() == 2
	})
}

func TestNonRetryableSkipsRetries(t *testing.T) {
	w := &fakeWriter{failTimes: 100, err: fmt.Errorf("insert 1 rows: %w", store.ErrNonRetryable)}
	obs := &fakeObs{}
	b := New(w, Config{BatchSize: 1, FlushInterval: time.Hour, BufferCapacity: 10, MaxRetries: 3}, nil, obs)
	defer b.Close(context.Background())

	b.Add(row("a"))

	eventually(t, 3*time.Second, func() bool {
		return obs.failed.Load() == 1 && obs.drop.Load() == 1
	})
	if got := w.calls.Load(); got != 1 {
		t.Errorf("non-retryable error should not be retried, got %d insert attempts", got)
	}
}

type partialWriter struct {
	mu           sync.Mutex
	failsLeft    int
	commitOnFail int
	inserted     []string
	callSizes    []int
}

func (w *partialWriter) InsertBatch(_ context.Context, rows []store.Row) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.callSizes = append(w.callSizes, len(rows))
	if w.failsLeft > 0 {
		w.failsLeft--
		n := min(w.commitOnFail, len(rows))
		w.record(rows[:n])
		return &store.PartialCommitError{Committed: n, Err: errors.New("chunk boundary failure")}
	}
	w.record(rows)
	return nil
}

func (w *partialWriter) record(rows []store.Row) {
	for _, r := range rows {
		w.inserted = append(w.inserted, r.QueryID)
	}
}

func (w *partialWriter) state() ([]string, []int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.inserted), slices.Clone(w.callSizes)
}

type chunkedWriter struct {
	mu        sync.Mutex
	chunkSize int
	delay     time.Duration
	failAt    int
	calls     [][]string
	inserted  []string
}

func (w *chunkedWriter) SplitBatch(rows []store.Row) [][]store.Row {
	var out [][]store.Row
	for i := 0; i < len(rows); i += w.chunkSize {
		out = append(out, rows[i:min(i+w.chunkSize, len(rows))])
	}
	return out
}

func (w *chunkedWriter) InsertBatch(ctx context.Context, rows []store.Row) error {
	if w.delay > 0 {
		select {
		case <-time.After(w.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.QueryID
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, ids)
	if len(w.calls) == w.failAt {
		return errors.New("chunk write failed")
	}
	w.inserted = append(w.inserted, ids...)
	return nil
}

func (w *chunkedWriter) state() ([]string, [][]string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.inserted), slices.Clone(w.calls)
}

func ids(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return out
}

func addAll(b *Buffer, list []string) {
	for _, id := range list {
		b.Add(row(id))
	}
}

func TestRetryAfterPartialCommitReplaysOnlyRemainder(t *testing.T) {
	w := &partialWriter{failsLeft: 1, commitOnFail: 2}
	obs := &fakeObs{}
	b := New(w, Config{BatchSize: 5, FlushInterval: time.Hour, BufferCapacity: 10, MaxRetries: 3}, nil, obs)
	defer b.Close(context.Background())

	want := ids("q", 5)
	addAll(b, want)

	eventually(t, 3*time.Second, func() bool { return obs.flushed.Load() == 5 })

	inserted, sizes := w.state()
	if !slices.Equal(inserted, want) {
		t.Errorf("inserted %v, want %v exactly once each", inserted, want)
	}
	if !slices.Equal(sizes, []int{5, 3}) {
		t.Errorf("retry sizes %v, want [5 3] — the committed prefix must not be replayed", sizes)
	}
	if obs.drop.Load() != 0 {
		t.Errorf("nothing should be dropped, got %d", obs.drop.Load())
	}
}

func TestRetryReplaysOnlyUncommittedChunks(t *testing.T) {
	w := &chunkedWriter{chunkSize: 2, failAt: 2}
	obs := &fakeObs{}
	b := New(w, Config{BatchSize: 6, FlushInterval: time.Hour, BufferCapacity: 10, MaxRetries: 3}, nil, obs)
	defer b.Close(context.Background())

	want := ids("q", 6)
	addAll(b, want)

	eventually(t, 3*time.Second, func() bool { return obs.flushed.Load() == 6 })

	inserted, calls := w.state()
	if !slices.Equal(inserted, want) {
		t.Errorf("inserted %v, want %v exactly once each", inserted, want)
	}
	wantCalls := [][]string{{"q-0", "q-1"}, {"q-2", "q-3"}, {"q-2", "q-3"}, {"q-4", "q-5"}}
	if len(calls) != len(wantCalls) {
		t.Fatalf("statements %v, want %v", calls, wantCalls)
	}
	for i, c := range calls {
		if !slices.Equal(c, wantCalls[i]) {
			t.Errorf("statement %d = %v, want %v", i, c, wantCalls[i])
		}
	}
	if obs.drop.Load() != 0 {
		t.Errorf("nothing should be dropped, got %d", obs.drop.Load())
	}
}

func TestFlushTimeoutAppliesPerChunk(t *testing.T) {
	const perChunk = 30 * time.Millisecond
	w := &chunkedWriter{chunkSize: 2, delay: perChunk}
	obs := &fakeObs{}
	b := New(w, Config{
		BatchSize: 12, FlushInterval: time.Hour, BufferCapacity: 20,
		MaxRetries: 0, FlushTimeout: 5 * perChunk,
	}, nil, obs)
	defer b.Close(context.Background())

	want := ids("q", 12)
	addAll(b, want)

	eventually(t, 5*time.Second, func() bool { return obs.flushed.Load() == 12 })

	inserted, calls := w.state()
	if len(calls) != 6 {
		t.Errorf("expected one statement per chunk, got %d", len(calls))
	}
	if !slices.Equal(inserted, want) {
		t.Errorf("inserted %v, want %v — a whole-batch deadline expired mid-batch", inserted, want)
	}
	if obs.failed.Load() != 0 || obs.drop.Load() != 0 {
		t.Errorf("no chunk should have timed out: failures=%d dropped=%d", obs.failed.Load(), obs.drop.Load())
	}
}

type ctxAwareWriter struct {
	started chan struct{}
	gotErr  chan error
}

func (w *ctxAwareWriter) InsertBatch(ctx context.Context, _ []store.Row) error {
	close(w.started)
	<-ctx.Done()
	w.gotErr <- ctx.Err()
	return ctx.Err()
}

func TestCloseCancelsInflightFlush(t *testing.T) {
	w := &ctxAwareWriter{started: make(chan struct{}), gotErr: make(chan error, 1)}
	b := New(w, Config{BatchSize: 1, FlushInterval: time.Hour, BufferCapacity: 10, MaxRetries: 0, FlushTimeout: time.Hour}, nil, nil)

	b.Add(row("a"))
	<-w.started

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = b.Close(ctx)

	select {
	case err := <-w.gotErr:
		if err == nil {
			t.Fatal("InsertBatch should have observed a cancelled context")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("InsertBatch was not cancelled by the drain budget")
	}
}

func TestDropWhenFull(t *testing.T) {
	w := &fakeWriter{started: make(chan struct{}, 10), block: make(chan struct{})}
	obs := &fakeObs{}
	b := New(w, Config{BatchSize: 1, FlushInterval: time.Hour, BufferCapacity: 1}, nil, obs)

	b.Add(row("a"))
	<-w.started

	b.Add(row("b"))
	b.Add(row("c"))

	eventually(t, time.Second, func() bool { return obs.drop.Load() >= 1 })

	close(w.block)
	_ = b.Close(context.Background())
}
