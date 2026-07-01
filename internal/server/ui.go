package server

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Shadi/trino-query-log-sink/internal/event"
	"github.com/Shadi/trino-query-log-sink/internal/store"
)

const (
	uiLimit    = 100
	maxUIPages = 50
)

type rangeOption struct{ Value, Label string }

var ranges = []rangeOption{
	{"1h", "Last hour"},
	{"6h", "Last 6 hours"},
	{"24h", "Last 24 hours"},
	{"7d", "Last 7 days"},
	{"30d", "Last 30 days"},
}

var rangeDurations = map[string]time.Duration{
	"1h":  time.Hour,
	"6h":  6 * time.Hour,
	"24h": 24 * time.Hour,
	"7d":  7 * 24 * time.Hour,
	"30d": 30 * 24 * time.Hour,
}

var rangeLabels = func() map[string]string {
	m := make(map[string]string, len(ranges))
	for _, r := range ranges {
		m[r.Value] = r.Label
	}
	return m
}()

var statesList = []string{"", "FINISHED", "FAILED", "CANCELED"}

var validSorts = map[string]bool{"start": true, "wall": true, "cpu": true, "bytes": true, "mem": true, "rows": true}

func rangeInfo(v string) (time.Duration, string, bool) {
	d, ok := rangeDurations[v]
	if !ok {
		return 0, "", false
	}
	return d, rangeLabels[v], true
}

type listView struct {
	Rows       []store.QuerySummary
	Ranges     []rangeOption
	States     []string
	Range      string
	RangeLabel string
	User       string
	Catalog    string
	State      string
	Sort       string
	Dir        string
	FailedOnly bool
	Count      int
	Error      string
	Page       int
	HasPrev    bool
	HasNext    bool
}

type detailView struct {
	Row    *store.Row
	Inputs []event.InputMetadata
}

func resolveRange(q url.Values) (string, time.Duration, string) {
	rng := q.Get("range")
	dur, label, ok := rangeInfo(rng)
	if !ok {
		rng = "24h"
		dur, label, _ = rangeInfo(rng)
	}
	return rng, dur, label
}

func buildQueryFilter(q url.Values) store.QueryFilter {
	_, dur, _ := resolveRange(q)
	sort := q.Get("sort")
	if !validSorts[sort] {
		sort = "start"
	}
	until := time.Now().UTC()
	return store.QueryFilter{
		Since:   until.Add(-dur),
		Until:   until,
		User:    strings.TrimSpace(q.Get("user")),
		Catalog: strings.TrimSpace(q.Get("catalog")),
		State:   q.Get("state"),
		Sort:    store.SortKey(sort),
		Desc:    q.Get("dir") != "asc",
	}
}

func (s *Server) buildListView(q url.Values) (*listView, store.QueryFilter) {
	rng, _, label := resolveRange(q)
	f := buildQueryFilter(q)

	page := 1
	if p, err := strconv.Atoi(q.Get("page")); err == nil && p > 1 {
		page = p
	}
	if page > maxUIPages {
		page = maxUIPages
	}
	f.Limit = uiLimit + 1
	f.Offset = (page - 1) * uiLimit

	dir := "asc"
	if f.Desc {
		dir = "desc"
	}
	v := &listView{
		Ranges: ranges, States: statesList,
		Range: rng, RangeLabel: label, User: f.User, Catalog: f.Catalog, State: f.State,
		Sort: string(f.Sort), Dir: dir, FailedOnly: f.State == "FAILED", Page: page,
	}
	return v, f
}

func (s *Server) listAndTrim(ctx context.Context, f store.QueryFilter, pageSize int) ([]store.QuerySummary, bool, error) {
	rows, err := s.store.ListQueries(ctx, f)
	if err != nil {
		return nil, false, err
	}
	hasNext := false
	if len(rows) > pageSize {
		rows = rows[:pageSize]
		hasNext = true
	}
	return rows, hasNext, nil
}

func (s *Server) populateRows(ctx context.Context, v *listView, f store.QueryFilter) {
	rows, hasNext, err := s.listAndTrim(ctx, f, uiLimit)
	if err != nil {
		v.Error = "query failed: " + err.Error()
		s.log.Error("list queries failed", "error", err)
		return
	}
	v.HasPrev = v.Page > 1
	v.HasNext = hasNext && v.Page < maxUIPages
	v.Rows = rows
	v.Count = len(rows)
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	v, f := s.buildListView(r.URL.Query())
	s.populateRows(r.Context(), v, f)
	s.render(w, s.dashboardTmpl, "base", v)
}

func (s *Server) handleQueriesPartial(w http.ResponseWriter, r *http.Request) {
	v, f := s.buildListView(r.URL.Query())
	s.populateRows(r.Context(), v, f)
	s.render(w, s.partialTmpl, "results", v)
}

func (s *Server) handleQueryDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("queryId")
	row, err := s.store.GetQuery(r.Context(), id)
	if err != nil {
		s.log.Error("get query failed", "query_id", id, "error", err)
		http.Error(w, "query lookup failed", http.StatusInternalServerError)
		return
	}
	if row == nil {
		http.Error(w, "query not found: "+id, http.StatusNotFound)
		return
	}

	var inputs []event.InputMetadata
	if row.InputsJSON != "" {
		_ = json.Unmarshal([]byte(row.InputsJSON), &inputs)
	}
	s.render(w, s.detailTmpl, "base", detailView{Row: row, Inputs: inputs})
}

func (v *listView) baseValues() url.Values {
	q := url.Values{}
	q.Set("range", v.Range)
	if v.User != "" {
		q.Set("user", v.User)
	}
	if v.Catalog != "" {
		q.Set("catalog", v.Catalog)
	}
	if v.State != "" {
		q.Set("state", v.State)
	}
	return q
}

func sortlink(v *listView, key, label string) template.HTML {
	q := v.baseValues()
	dir, arrow := "desc", ""
	if v.Sort == key {
		if v.Dir == "desc" {
			arrow, dir = " ▼", "asc"
		} else {
			arrow, dir = " ▲", "desc"
		}
	}
	q.Set("sort", key)
	q.Set("dir", dir)
	enc := template.HTMLEscapeString(q.Encode())
	return template.HTML(fmt.Sprintf(
		`<a href="/?%s" hx-get="/partials/queries?%s" hx-target="#results">%s%s</a>`,
		enc, enc, template.HTMLEscapeString(label), arrow))
}

func topHref(v *listView, key string) string { return "/?" + topValues(v, key).Encode() }
func topHrefPartial(v *listView, key string) string {
	return "/partials/queries?" + topValues(v, key).Encode()
}
func failedHref(v *listView) string        { return "/?" + failedValues(v).Encode() }
func failedHrefPartial(v *listView) string { return "/partials/queries?" + failedValues(v).Encode() }

func topValues(v *listView, key string) url.Values {
	q := v.baseValues()
	q.Set("sort", key)
	q.Set("dir", "desc")
	return q
}

func failedValues(v *listView) url.Values {
	q := v.baseValues()
	if v.FailedOnly {
		q.Del("state")
	} else {
		q.Set("state", "FAILED")
	}
	q.Set("sort", v.Sort)
	q.Set("dir", v.Dir)
	return q
}

func prevHref(v *listView) string { return "/?" + pageValues(v, v.Page-1).Encode() }
func prevHrefPartial(v *listView) string {
	return "/partials/queries?" + pageValues(v, v.Page-1).Encode()
}
func nextHref(v *listView) string { return "/?" + pageValues(v, v.Page+1).Encode() }
func nextHrefPartial(v *listView) string {
	return "/partials/queries?" + pageValues(v, v.Page+1).Encode()
}

func pageValues(v *listView, page int) url.Values {
	q := v.baseValues()
	q.Set("sort", v.Sort)
	q.Set("dir", v.Dir)
	if page > 1 {
		q.Set("page", strconv.Itoa(page))
	}
	return q
}
