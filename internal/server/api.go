package server

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/Shadi/trino-query-log-sink/internal/event"
	"github.com/Shadi/trino-query-log-sink/internal/store"
)

const (
	apiDefaultLimit = 100
	apiMaxLimit     = 500
)

type listResponse struct {
	Queries []store.QuerySummary `json:"queries"`
	Count   int                  `json:"count"`
	Limit   int                  `json:"limit"`
	Offset  int                  `json:"offset"`
	HasNext bool                 `json:"hasNext"`
}

type queryResponse struct {
	*store.Row
	Inputs []event.InputMetadata `json:"inputs"`
}

type apiError struct {
	Error string `json:"error"`
}

func (s *Server) handleAPIListQueries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := buildQueryFilter(q)
	limit := clampLimit(q.Get("limit"))
	offset := parseOffset(q.Get("offset"))
	f.Limit = limit + 1
	f.Offset = offset

	rows, hasNext, err := s.listAndTrim(r.Context(), f, limit)
	if err != nil {
		s.log.Error("api list queries failed", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(w, http.StatusOK, listResponse{
		Queries: rows, Count: len(rows), Limit: limit, Offset: offset, HasNext: hasNext,
	})
}

func (s *Server) handleAPIGetQuery(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("queryId")
	row, err := s.store.GetQuery(r.Context(), id)
	if err != nil {
		s.log.Error("api get query failed", "query_id", id, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "query lookup failed")
		return
	}
	if row == nil {
		writeJSONError(w, http.StatusNotFound, "query not found: "+id)
		return
	}
	var inputs []event.InputMetadata
	if row.InputsJSON != "" {
		_ = json.Unmarshal([]byte(row.InputsJSON), &inputs)
	}
	writeJSON(w, http.StatusOK, queryResponse{Row: row, Inputs: inputs})
}

func clampLimit(v string) int {
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return apiDefaultLimit
	}
	if n > apiMaxLimit {
		return apiMaxLimit
	}
	return n
}

func parseOffset(v string) int {
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, apiError{Error: msg})
}
