package store

import "time"

const (
	defaultListLimit = 100
	maxListLimit     = 5000
)

type predicate struct {
	column string
	op     string
	value  any
}

func listPredicates(f QueryFilter) []predicate {
	var ps []predicate
	if !f.Since.IsZero() {
		ps = append(ps, predicate{"create_time", ">=", f.Since})
	}
	if !f.Until.IsZero() {
		ps = append(ps, predicate{"create_time", "<", f.Until})
	}
	if f.User != "" {
		ps = append(ps, predicate{"user_name", "=", f.User})
	}
	if f.Catalog != "" {
		ps = append(ps, predicate{"catalog", "=", f.Catalog})
	}
	if f.State != "" {
		ps = append(ps, predicate{"query_state", "=", f.State})
	}
	return ps
}

func queryIDPredicates(queryID string) ([]predicate, bool) {
	from, to, ok := queryIDWindow(queryID)
	if !ok {
		return nil, false
	}
	return []predicate{
		{"query_id", "=", queryID},
		{"create_time", ">=", from},
		{"create_time", "<", to},
	}, true
}

func sortColumn(f QueryFilter) string {
	if col, ok := sortColumns[f.Sort]; ok {
		return col
	}
	return "create_time"
}

func orderBy(f QueryFilter) string {
	dir := "ASC"
	if f.Desc {
		dir = "DESC"
	}
	col := sortColumn(f)
	clause := quoteIdent(col) + " " + dir
	if col != "query_id" {
		clause += ", " + quoteIdent("query_id") + " " + dir
	}
	return clause
}

func pageLimit(f QueryFilter) int {
	if f.Limit <= 0 || f.Limit > maxListLimit {
		return defaultListLimit
	}
	return f.Limit
}

func queryIDWindow(queryID string) (from, to time.Time, ok bool) {
	day, ok := queryIDDay(queryID)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	return day.AddDate(0, 0, -1), day.AddDate(0, 0, 2), true
}

func (q *QuerySummary) scanDests() []any {
	return []any{
		&q.QueryID, &q.CreateTime, &q.ExecutionStartTime, &q.UserName, &q.Source,
		&q.Catalog, &q.QueryState, &q.QueryType, &q.QueryPreview, &q.WallMS, &q.CPUMS,
		&q.PhysicalInputBytes, &q.PeakUserMemoryBytes, &q.OutputRows, &q.ProcessedInputRows, &q.ErrorCode,
	}
}
