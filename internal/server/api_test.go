package server

import "testing"

func TestClampLimitAndOffset(t *testing.T) {
	for in, want := range map[string]int{"": apiDefaultLimit, "x": apiDefaultLimit, "0": apiDefaultLimit, "-1": apiDefaultLimit, "7": 7, "500": 500, "501": apiMaxLimit} {
		if got := clampLimit(in); got != want {
			t.Errorf("clampLimit(%q) = %d, want %d", in, got, want)
		}
	}
	for in, want := range map[string]int{"": 0, "x": 0, "-5": 0, "42": 42, "100000": apiMaxOffset, "2000000000": apiMaxOffset} {
		if got := parseOffset(in); got != want {
			t.Errorf("parseOffset(%q) = %d, want %d", in, got, want)
		}
	}
}
