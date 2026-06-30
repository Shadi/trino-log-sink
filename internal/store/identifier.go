package store

import (
	"strings"
	"time"
)

func queryIDDay(queryID string) (time.Time, bool) {
	if len(queryID) < 8 {
		return time.Time{}, false
	}
	d, err := time.Parse("20060102", queryID[:8])
	if err != nil {
		return time.Time{}, false
	}
	return d, true
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func qualifiedName(catalog, schema, table string) string {
	return quoteIdent(catalog) + "." + quoteIdent(schema) + "." + quoteIdent(table)
}
