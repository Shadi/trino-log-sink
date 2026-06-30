package prune

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

type fakePruner struct {
	cutoff time.Time
	called bool
	err    error
}

func (f *fakePruner) Prune(_ context.Context, olderThan time.Time) error {
	f.called = true
	f.cutoff = olderThan
	return f.err
}

func TestRunComputesCutoff(t *testing.T) {
	now := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	p := &fakePruner{}
	if err := Run(context.Background(), p, 7, now, slog.Default()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := now.AddDate(0, 0, -7)
	if !p.called || !p.cutoff.Equal(want) {
		t.Errorf("cutoff = %v, want %v (called=%v)", p.cutoff, want, p.called)
	}
}

func TestRunPropagatesError(t *testing.T) {
	p := &fakePruner{err: errors.New("boom")}
	if err := Run(context.Background(), p, 7, time.Now(), slog.Default()); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunRejectsNegativeRetention(t *testing.T) {
	p := &fakePruner{}
	if err := Run(context.Background(), p, -1, time.Now(), slog.Default()); err == nil {
		t.Fatal("expected error for negative retention")
	}
	if p.called {
		t.Error("should not call Prune on invalid retention")
	}
}
