package store

import (
	"errors"
	"fmt"
)

// ErrNonRetryable marks insert failures that are deterministic — replaying the
// same statement can never succeed, so callers should drop the batch instead
// of burning retries.
var ErrNonRetryable = errors.New("non-retryable")

type PartialCommitError struct {
	Committed int
	Err       error
}

func (e *PartialCommitError) Error() string {
	return fmt.Sprintf("%d rows committed before failure: %v", e.Committed, e.Err)
}

func (e *PartialCommitError) Unwrap() error { return e.Err }

func CommittedRows(err error) int {
	var pce *PartialCommitError
	if errors.As(err, &pce) {
		return pce.Committed
	}
	return 0
}
