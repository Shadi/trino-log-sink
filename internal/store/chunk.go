package store

import (
	"strings"
	"time"
)

// The constants below upper-bound the serialized size of each arg (including
// its " USING "-list separator) so batches can be split before a server
// rejects them for size.
const (
	stringArgOverhead = 4  // two quotes + ", " separator
	nullArgBytes      = 6  // "NULL" + separator
	numericArgBytes   = 24 // int64 decimal (<= 20 chars) + separator
	timestampArgBytes = 64 // "TIMESTAMP '...'" (<= 50 bytes) + separator
)

// estimateArgBytes upper-bounds the bytes a driver adds to a statement for one
// arg. Strings gain one byte per embedded single quote (quote doubling).
func estimateArgBytes(a any) int {
	switch v := a.(type) {
	case string:
		return len(v) + strings.Count(v, "'") + stringArgOverhead
	case nil:
		return nullArgBytes
	case time.Time:
		return timestampArgBytes
	default:
		return numericArgBytes
	}
}

// chunkRows greedily packs rows into chunks whose estimated sizes sum to at
// most budget. Order is preserved and chunks are never empty; a single row
// estimated above budget becomes its own chunk.
func chunkRows(rows []Row, budget int, estimate func(Row) int) [][]Row {
	var chunks [][]Row
	var cur []Row
	size := 0
	for _, r := range rows {
		n := estimate(r)
		if len(cur) > 0 && size+n > budget {
			chunks = append(chunks, cur)
			cur, size = nil, 0
		}
		cur = append(cur, r)
		size += n
	}
	if len(cur) > 0 {
		chunks = append(chunks, cur)
	}
	return chunks
}
