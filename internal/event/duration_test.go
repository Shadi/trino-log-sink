package event

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParseISO8601Duration(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"PT0S", 0},
		{"PT1.234S", 1234 * time.Millisecond},
		{"PT0.5S", 500 * time.Millisecond},
		{"PT5M", 5 * time.Minute},
		{"PT2H", 2 * time.Hour},
		{"PT5H30M15.5S", 5*time.Hour + 30*time.Minute + 15500*time.Millisecond},
		{"PT100H", 100 * time.Hour},
		{"-PT1.5S", -1500 * time.Millisecond},
		{"", 0},
	}
	for _, c := range cases {
		got, err := parseISO8601Duration(c.in)
		if err != nil {
			t.Errorf("parseISO8601Duration(%q) unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseISO8601Duration(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestDurationUnmarshalJSON(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"PT1.234S"`), &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if d.Millis() != 1234 {
		t.Errorf("Millis() = %d, want 1234", d.Millis())
	}

	d = 0
	if err := json.Unmarshal([]byte(`"garbage"`), &d); err != nil {
		t.Fatalf("unmarshal garbage should be tolerant, got: %v", err)
	}
	if d.Millis() != 0 {
		t.Errorf("garbage Millis() = %d, want 0", d.Millis())
	}

	d = 0
	if err := json.Unmarshal([]byte(`2.5`), &d); err != nil {
		t.Fatalf("unmarshal numeric: %v", err)
	}
	if d.Millis() != 2500 {
		t.Errorf("numeric Millis() = %d, want 2500", d.Millis())
	}

	d = 0
	if err := json.Unmarshal([]byte(`null`), &d); err != nil {
		t.Fatalf("unmarshal null: %v", err)
	}
	if d.Millis() != 0 {
		t.Errorf("null Millis() = %d, want 0", d.Millis())
	}
}

func TestParseISO8601DurationInvalid(t *testing.T) {
	for _, in := range []string{"abc", "1.234S", "P1X", "PTX"} {
		if _, err := parseISO8601Duration(in); err == nil {
			t.Errorf("parseISO8601Duration(%q) expected error, got nil", in)
		}
	}
}
