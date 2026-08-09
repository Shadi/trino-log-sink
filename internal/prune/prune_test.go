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

func TestRunComputesMidnightAlignedCutoff(t *testing.T) {
	cases := []struct {
		name          string
		now           time.Time
		retentionDays int
		want          time.Time
	}{
		{
			name:          "now already at midnight",
			now:           time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC),
			retentionDays: 7,
			want:          time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "cronjob hour rounds back to midnight",
			now:           time.Date(2026, 6, 30, 3, 0, 0, 0, time.UTC),
			retentionDays: 7,
			want:          time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "last minute of the day rounds back to midnight",
			now:           time.Date(2026, 6, 30, 23, 59, 59, 999999999, time.UTC),
			retentionDays: 7,
			want:          time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "two day retention mid day keeps two whole days plus today",
			now:           time.Date(2026, 6, 30, 3, 0, 0, 0, time.UTC),
			retentionDays: 2,
			want:          time.Date(2026, 6, 28, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "zero retention keeps today's partition",
			now:           time.Date(2026, 6, 30, 3, 0, 0, 0, time.UTC),
			retentionDays: 0,
			want:          time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "cutoff crosses a month boundary",
			now:           time.Date(2026, 7, 1, 5, 30, 0, 0, time.UTC),
			retentionDays: 7,
			want:          time.Date(2026, 6, 24, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "non utc now is aligned on the utc day",
			now:           time.Date(2026, 6, 30, 2, 0, 0, 0, time.FixedZone("UTC+3", 3*60*60)),
			retentionDays: 7,
			want:          time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &fakePruner{}
			if err := Run(context.Background(), p, tc.retentionDays, tc.now, slog.Default()); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if !p.called {
				t.Fatal("Prune was not called")
			}
			if !p.cutoff.Equal(tc.want) {
				t.Errorf("cutoff = %v, want %v", p.cutoff, tc.want)
			}
			if h, m, s := p.cutoff.UTC().Clock(); h != 0 || m != 0 || s != 0 || p.cutoff.UTC().Nanosecond() != 0 {
				t.Errorf("cutoff %v is not aligned to a UTC day boundary", p.cutoff)
			}
		})
	}
}

func TestRunNeverPrunesInsideRetentionWindow(t *testing.T) {
	day := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	for _, retentionDays := range []int{0, 1, 2, 7, 30} {
		for hour := 0; hour < 24; hour++ {
			now := day.Add(time.Duration(hour) * time.Hour)
			p := &fakePruner{}
			if err := Run(context.Background(), p, retentionDays, now, slog.Default()); err != nil {
				t.Fatalf("Run(retentionDays=%d, now=%v): %v", retentionDays, now, err)
			}
			rollingCutoff := now.AddDate(0, 0, -retentionDays)
			if p.cutoff.After(rollingCutoff) {
				t.Errorf("retentionDays=%d now=%v: cutoff %v deletes data newer than %v",
					retentionDays, now, p.cutoff, rollingCutoff)
			}
			if oldest := rollingCutoff.AddDate(0, 0, -1); !p.cutoff.After(oldest) {
				t.Errorf("retentionDays=%d now=%v: cutoff %v retains more than one extra day beyond %v",
					retentionDays, now, p.cutoff, rollingCutoff)
			}
		}
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
