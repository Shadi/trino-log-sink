package store

import (
	"fmt"
	"unicode/utf8"
)

// maxSmallFieldBytes caps the string fields that are never legitimately large
// (names, tags, identifiers). They are still client-controlled — the ingest
// body limit is 32MB — so without a cap a single absurd field could defeat the
// statement-size budget.
const maxSmallFieldBytes = 16_384

// truncate returns s cut to at most max bytes, never more. The cut lands on a
// UTF-8 rune boundary and, when it fits, a "\n…[truncated N bytes]" marker
// (N = bytes removed) replaces the tail; if max is smaller than the marker the
// result is a bare prefix, and max <= 0 yields "".
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 0 {
		return ""
	}
	// Sizing the marker with len(s) over-counts N's digits, so the final
	// result can only come in at or under max.
	cut := max - len(fmt.Sprintf("\n…[truncated %d bytes]", len(s)))
	if cut <= 0 {
		return s[:runeSafeCut(s, max)]
	}
	cut = runeSafeCut(s, cut)
	return s[:cut] + fmt.Sprintf("\n…[truncated %d bytes]", len(s)-cut)
}

// runeSafeCut backs cut up to the nearest rune start so a truncated string is
// never invalid UTF-8. cut must be < len(s).
func runeSafeCut(s string, cut int) int {
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return cut
}

// largeFields are the payload columns that can legitimately reach hundreds of
// kilobytes. Order matters: on equal lengths, clampRow shrinks earlier entries
// first, preferring the plans over inputs_json (whose truncation loses the
// inputs list in the API/UI).
func (r *Row) largeFields() []*string {
	return []*string{&r.Plan, &r.JSONPlan, &r.QueryText, &r.ErrorMessage, &r.InputsJSON}
}

func (r *Row) smallFields() []*string {
	return []*string{
		&r.QueryID, &r.QueryState, &r.QueryType, &r.UserName, &r.Source, &r.Principal,
		&r.ClientTags, &r.Catalog, &r.SchemaName, &r.UpdateType, &r.ErrorCode, &r.ErrorType,
		&r.ResourceGroup, &r.ServerVersion, &r.Environment,
	}
}

func (r *Row) stringFields() []*string {
	return append(r.largeFields(), r.smallFields()...)
}

// TruncateFields returns a copy of r with the large payload fields capped at
// max bytes each and every other string field capped at
// min(max, maxSmallFieldBytes). max <= 0 disables truncation.
func (r Row) TruncateFields(max int) Row {
	if max <= 0 {
		return r
	}
	small := min(max, maxSmallFieldBytes)
	for _, f := range r.largeFields() {
		*f = truncate(*f, max)
	}
	for _, f := range r.smallFields() {
		*f = truncate(*f, small)
	}
	return r
}
