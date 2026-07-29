package store

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTruncateShortStringUnchanged(t *testing.T) {
	for _, s := range []string{"", "hello", strings.Repeat("x", 100)} {
		if got := truncate(s, 100); got != s {
			t.Errorf("truncate(%q, 100) = %q, want unchanged", s, got)
		}
	}
}

func TestTruncateCapsAndAppendsMarker(t *testing.T) {
	s := strings.Repeat("x", 1000)
	got := truncate(s, 200)

	if len(got) > 200 {
		t.Fatalf("len = %d, want <= 200", len(got))
	}
	idx := strings.Index(got, "\n…[truncated ")
	if idx < 0 {
		t.Fatalf("marker missing: %q", got[max(0, len(got)-40):])
	}
	var n int
	if _, err := fmt.Sscanf(got[idx:], "\n…[truncated %d bytes]", &n); err != nil {
		t.Fatalf("marker malformed: %v", err)
	}
	if want := len(s) - idx; n != want {
		t.Errorf("marker says %d bytes removed, want %d", n, want)
	}
}

func TestTruncateTinyMax(t *testing.T) {
	s := strings.Repeat("x", 100)
	for _, m := range []int{-1, 0, 1, 5, 10, 20} {
		got := truncate(s, m)
		if len(got) > max(m, 0) {
			t.Errorf("truncate(_, %d) returned %d bytes", m, len(got))
		}
	}
	if got := truncate(s, 0); got != "" {
		t.Errorf("truncate(_, 0) = %q, want empty", got)
	}
}

func TestTruncateRuneBoundarySafe(t *testing.T) {
	s := strings.Repeat("é", 300) // 2-byte runes: every odd byte offset splits a rune
	for m := 0; m <= 120; m++ {
		got := truncate(s, m)
		if len(got) > m && m >= 0 {
			t.Fatalf("truncate(_, %d) returned %d bytes", m, len(got))
		}
		if !utf8.ValidString(got) {
			t.Fatalf("truncate(_, %d) produced invalid UTF-8", m)
		}
	}
}

func TestTruncateFields(t *testing.T) {
	r := Row{
		QueryID:    "q-1",
		QueryText:  strings.Repeat("q", 400_000),
		Plan:       strings.Repeat("p", 400_000),
		JSONPlan:   strings.Repeat("j", 400_000),
		ClientTags: strings.Repeat("t", 20_000),
		UserName:   "alice",
	}
	got := r.TruncateFields(300_000)

	for name, f := range map[string]string{"query_text": got.QueryText, "plan": got.Plan, "json_plan": got.JSONPlan} {
		if len(f) > 300_000 {
			t.Errorf("%s not capped: %d bytes", name, len(f))
		}
		if !strings.Contains(f, "[truncated ") {
			t.Errorf("%s missing truncation marker", name)
		}
	}
	if len(got.ClientTags) > maxSmallFieldBytes {
		t.Errorf("client_tags not capped at %d: %d bytes", maxSmallFieldBytes, len(got.ClientTags))
	}
	if got.QueryID != "q-1" || got.UserName != "alice" {
		t.Errorf("small intact fields changed: %q %q", got.QueryID, got.UserName)
	}

	if noop := r.TruncateFields(0); noop != r {
		t.Errorf("max <= 0 should be a no-op")
	}
}
