// Package ingest buffers query-log rows in memory and flushes them to the store
// in batches. A single flusher goroutine owns the batch slice, so no locking is
// needed; producers hand rows over a buffered channel with a non-blocking send
// so the HTTP /ingest path never blocks on Trino.
package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shadi/trino-log-sink/internal/store"
)

// Writer is the subset of store.Store the buffer needs. Narrowing it keeps the
// buffer trivially fakeable in tests.
type Writer interface {
	InsertBatch(ctx context.Context, rows []store.Row) error
}

type batchSplitter interface {
	SplitBatch(rows []store.Row) [][]store.Row
}

// Observer receives buffer lifecycle counts. Implementations must be safe for
// concurrent use; the no-op default is used when none is supplied.
type Observer interface {
	Enqueued()
	Dropped(n int)
	Flushed(n int)
	FlushFailed()
}

type NopObserver struct{}

func (NopObserver) Enqueued()    {}
func (NopObserver) Dropped(int)  {}
func (NopObserver) Flushed(int)  {}
func (NopObserver) FlushFailed() {}

type Config struct {
	BatchSize      int
	FlushInterval  time.Duration
	BufferCapacity int
	MaxRetries     int
	FlushTimeout   time.Duration
}

type Buffer struct {
	w   Writer
	cfg Config
	log *slog.Logger
	obs Observer

	ch       chan store.Row
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	flushBase   context.Context
	flushCancel context.CancelFunc

	lastFlushUnixMs     atomic.Int64
	lastFlushDurationMs atomic.Int64
}

func New(w Writer, cfg Config, log *slog.Logger, obs Observer) *Buffer {
	if log == nil {
		log = slog.Default()
	}
	if obs == nil {
		obs = NopObserver{}
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = 30 * time.Second
	}
	if cfg.BufferCapacity < 1 {
		cfg.BufferCapacity = 1
	}
	b := &Buffer{
		w:    w,
		cfg:  cfg,
		log:  log,
		obs:  obs,
		ch:   make(chan store.Row, cfg.BufferCapacity),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	b.flushBase, b.flushCancel = context.WithCancel(context.Background())
	go b.run()
	return b
}

func (b *Buffer) Add(r store.Row) {
	select {
	case b.ch <- r:
		b.obs.Enqueued()
	case <-b.stop:
		b.obs.Dropped(1)
	default:
		b.obs.Dropped(1)
		b.log.Warn("ingest buffer full, dropping event", "query_id", r.QueryID)
	}
}

func (b *Buffer) Close(ctx context.Context) error {
	b.stopOnce.Do(func() { close(b.stop) })
	select {
	case <-b.done:
		return nil
	case <-ctx.Done():
		b.flushCancel()
		<-b.done
		return ctx.Err()
	}
}

func (b *Buffer) Stats() (depth, capacity int, lastFlushUnixMs, lastFlushDurationMs int64) {
	return len(b.ch), cap(b.ch), b.lastFlushUnixMs.Load(), b.lastFlushDurationMs.Load()
}

func (b *Buffer) run() {
	defer close(b.done)
	defer b.flushCancel()
	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]store.Row, 0, b.cfg.BatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		b.flush(batch)
		batch = batch[:0]
	}

	for {
		select {
		case r := <-b.ch:
			batch = append(batch, r)
			if len(batch) >= b.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-b.stop:
			b.drain(&batch)
			flush()
			return
		}
	}
}

func (b *Buffer) drain(batch *[]store.Row) {
	for {
		select {
		case r := <-b.ch:
			*batch = append(*batch, r)
			if len(*batch) >= b.cfg.BatchSize {
				b.flush(*batch)
				*batch = (*batch)[:0]
			}
		default:
			return
		}
	}
}

func (b *Buffer) flush(rows []store.Row) {
	rows = dedupe(rows)

	backoff := 100 * time.Millisecond
	for attempt := 0; attempt <= b.cfg.MaxRetries; attempt++ {
		start := time.Now()
		committed, err := b.insert(rows)
		committed = min(max(committed, 0), len(rows))
		if committed > 0 {
			b.lastFlushUnixMs.Store(time.Now().UnixMilli())
			b.lastFlushDurationMs.Store(time.Since(start).Milliseconds())
			b.obs.Flushed(committed)
			rows = rows[committed:]
		}
		if err == nil {
			return
		}
		b.log.Error("flush failed",
			"attempt", attempt+1, "committed", committed, "remaining", len(rows), "error", err)
		if errors.Is(err, store.ErrNonRetryable) {
			b.log.Error("non-retryable flush error, skipping retries", "rows", len(rows))
			break
		}
		if b.flushBase.Err() != nil {
			break
		}
		if attempt < b.cfg.MaxRetries {
			select {
			case <-time.After(backoff):
			case <-b.stop:
			}
			backoff *= 2
		}
	}
	b.obs.FlushFailed()
	b.obs.Dropped(len(rows))
	b.log.Error("dropping batch after exhausting retries", "rows", len(rows))
}

func (b *Buffer) insert(rows []store.Row) (int, error) {
	committed := 0
	for _, chunk := range b.split(rows) {
		ctx, cancel := context.WithTimeout(b.flushBase, b.cfg.FlushTimeout)
		err := b.w.InsertBatch(ctx, chunk)
		cancel()
		if err != nil {
			return committed + store.CommittedRows(err), err
		}
		committed += len(chunk)
	}
	return committed, nil
}

func (b *Buffer) split(rows []store.Row) [][]store.Row {
	if s, ok := b.w.(batchSplitter); ok {
		if chunks := s.SplitBatch(rows); len(chunks) > 0 {
			return chunks
		}
	}
	return [][]store.Row{rows}
}

func dedupe(rows []store.Row) []store.Row {
	seen := make(map[string]int, len(rows))
	out := make([]store.Row, 0, len(rows))
	for _, r := range rows {
		if i, ok := seen[r.QueryID]; ok {
			out[i] = r
			continue
		}
		seen[r.QueryID] = len(out)
		out = append(out, r)
	}
	return out
}
