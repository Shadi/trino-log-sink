package observability

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type BufferStats struct {
	Depth               int
	Capacity            int
	LastFlushUnixMs     int64
	LastFlushDurationMs int64
}

type Metrics struct {
	eventsReceived   atomic.Int64
	eventsInvalid    atomic.Int64
	eventsSuppressed atomic.Int64
	rowsEnqueued     atomic.Int64
	eventsDropped    atomic.Int64
	batchesFlushed   atomic.Int64
	rowsFlushed      atomic.Int64
	flushFailures    atomic.Int64

	bufferSource atomic.Pointer[func() BufferStats]
}

func NewMetrics() *Metrics { return &Metrics{} }

func (m *Metrics) SetBufferSource(f func() BufferStats) { m.bufferSource.Store(&f) }

func (m *Metrics) ReceivedEvent()   { m.eventsReceived.Add(1) }
func (m *Metrics) InvalidEvent()    { m.eventsInvalid.Add(1) }
func (m *Metrics) SuppressedEvent() { m.eventsSuppressed.Add(1) }

func (m *Metrics) Enqueued()     { m.rowsEnqueued.Add(1) }
func (m *Metrics) Dropped()      { m.eventsDropped.Add(1) }
func (m *Metrics) Flushed(n int) { m.batchesFlushed.Add(1); m.rowsFlushed.Add(int64(n)) }
func (m *Metrics) FlushFailed()  { m.flushFailures.Add(1) }

const metricPrefix = "trino_query_log_"

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	counters := []struct {
		name string
		help string
		val  int64
	}{
		{"events_received_total", "Query completed events received on /ingest.", m.eventsReceived.Load()},
		{"events_invalid_total", "Events skipped because they failed validation.", m.eventsInvalid.Load()},
		{"events_suppressed_total", "Events dropped because their source matched this service.", m.eventsSuppressed.Load()},
		{"rows_enqueued_total", "Rows accepted into the in-memory buffer.", m.rowsEnqueued.Load()},
		{"events_dropped_total", "Rows dropped because the buffer was full or closing.", m.eventsDropped.Load()},
		{"batches_flushed_total", "Batches successfully written to Trino.", m.batchesFlushed.Load()},
		{"rows_flushed_total", "Rows successfully written to Trino.", m.rowsFlushed.Load()},
		{"flush_failures_total", "Batches dropped after exhausting flush retries.", m.flushFailures.Load()},
	}
	for _, c := range counters {
		fmt.Fprintf(w, "# HELP %s%s %s\n", metricPrefix, c.name, c.help)
		fmt.Fprintf(w, "# TYPE %s%s counter\n", metricPrefix, c.name)
		fmt.Fprintf(w, "%s%s %d\n", metricPrefix, c.name, c.val)
	}

	src := m.bufferSource.Load()
	if src == nil {
		return
	}
	s := (*src)()
	gauges := []struct {
		name string
		help string
		val  int64
	}{
		{"buffer_depth", "Rows currently queued in the in-memory buffer.", int64(s.Depth)},
		{"buffer_capacity", "Maximum rows the in-memory buffer can hold.", int64(s.Capacity)},
		{"last_flush_timestamp_seconds", "Unix time of the last successful flush.", s.LastFlushUnixMs / 1000},
		{"last_flush_duration_ms", "Duration of the last successful flush in milliseconds.", s.LastFlushDurationMs},
	}
	for _, g := range gauges {
		fmt.Fprintf(w, "# HELP %s%s %s\n", metricPrefix, g.name, g.help)
		fmt.Fprintf(w, "# TYPE %s%s gauge\n", metricPrefix, g.name)
		fmt.Fprintf(w, "%s%s %d\n", metricPrefix, g.name, g.val)
	}
}
