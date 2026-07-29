package ingest

import (
	"context"
	"errors"
	"fmt"
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
