// Package prune implements retention: deleting query-log rows older than a
// configured age. It is invoked by the prune subcommand (run as a Kubernetes
// CronJob); the serving process never deletes.
package prune

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Pruner is the slice of the store prune needs.
type Pruner interface {
	Prune(ctx context.Context, olderThan time.Time) error
}

func Run(ctx context.Context, p Pruner, retentionDays int, now time.Time, log *slog.Logger) error {
	if retentionDays < 0 {
		return fmt.Errorf("retentionDays must be >= 0, got %d", retentionDays)
	}
	cutoff := now.Add(-time.Duration(retentionDays) * 24 * time.Hour)
	log.Info("pruning query log", "older_than", cutoff.Format(time.RFC3339), "retention_days", retentionDays)
	if err := p.Prune(ctx, cutoff); err != nil {
		return fmt.Errorf("prune: %w", err)
	}
	log.Info("prune complete", "older_than", cutoff.Format(time.RFC3339))
	return nil
}
