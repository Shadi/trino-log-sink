package store

import "strings"

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func qualifiedName(catalog, schema, table string) string {
	return quoteIdent(catalog) + "." + quoteIdent(schema) + "." + quoteIdent(table)
}
