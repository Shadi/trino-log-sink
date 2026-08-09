package server

import (
	"encoding/json"
	"net/http"

	"github.com/Shadi/trino-log-sink/internal/event"
	"github.com/Shadi/trino-log-sink/internal/store"
)

const maxIngestBytes = 32 << 20

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	s.metrics.ReceivedEvent()

	r.Body = http.MaxBytesReader(w, r.Body, maxIngestBytes)
	var e event.Event
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		s.metrics.InvalidEvent()
		s.log.Warn("ingest: malformed event, skipping", "error", err)
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if err := e.Validate(); err != nil {
		s.metrics.InvalidEvent()
		s.log.Warn("ingest: invalid event, skipping", "error", err)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	if e.SourceValue() == s.cfg.Trino.Source {
		s.metrics.SuppressedEvent()
		w.WriteHeader(http.StatusAccepted)
		return
	}

	row := store.RowFromEvent(&e).
		ApplyPlanPolicy(store.PlanPolicy{Capture: s.cfg.PlanCapture, MinWall: s.cfg.PlanCaptureMinWall}).
		WithPreview(s.cfg.PreviewBytes).
		TruncateFields(s.cfg.MaxFieldBytes)
	s.buf.Add(row)
	w.WriteHeader(http.StatusAccepted)
}
