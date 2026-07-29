package observability

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDroppedAddsCount(t *testing.T) {
	m := NewMetrics()
	m.Dropped(3)
	m.Dropped(2)

	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, nil)

	if body := rec.Body.String(); !strings.Contains(body, "trino_query_log_events_dropped_total 5") {
		t.Errorf("dropped counter should accumulate row counts, got:\n%s", body)
	}
}
