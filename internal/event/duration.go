package event

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*d = 0
		return nil
	}

	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		if dur, err := parseISO8601Duration(s); err == nil {
			*d = Duration(dur)
		} else {
			*d = 0
		}
		return nil
	}

	var secs float64
	if err := json.Unmarshal(b, &secs); err == nil {
		*d = Duration(time.Duration(secs * float64(time.Second)))
		return nil
	}

	*d = 0
	return nil
}

func (d Duration) Millis() int64 {
	return int64(time.Duration(d) / time.Millisecond)
}

func parseISO8601Duration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}

	neg := false
	switch s[0] {
	case '-':
		neg = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	if len(s) == 0 || s[0] != 'P' {
		return 0, fmt.Errorf("invalid ISO-8601 duration %q", s)
	}
	s = s[1:]

	var total time.Duration
	inTime := false
	i := 0
	for i < len(s) {
		if s[i] == 'T' {
			inTime = true
			i++
			continue
		}

		start := i
		for i < len(s) && (s[i] == '.' || s[i] == '-' || (s[i] >= '0' && s[i] <= '9')) {
			i++
		}
		if start == i || i >= len(s) {
			return 0, fmt.Errorf("invalid ISO-8601 duration %q", s)
		}
		num, err := strconv.ParseFloat(s[start:i], 64)
		if err != nil {
			return 0, fmt.Errorf("invalid number in duration %q: %w", s, err)
		}

		unit := s[i]
		i++

		var scale time.Duration
		switch unit {
		case 'D':
			scale = 24 * time.Hour
		case 'H':
			if !inTime {
				return 0, fmt.Errorf("H outside time part in %q", s)
			}
			scale = time.Hour
		case 'M':
			if !inTime {
				return 0, fmt.Errorf("M outside time part in %q", s)
			}
			scale = time.Minute
		case 'S':
			if !inTime {
				return 0, fmt.Errorf("S outside time part in %q", s)
			}
			scale = time.Second
		default:
			return 0, fmt.Errorf("unknown unit %q in duration %q", string(unit), s)
		}
		total += time.Duration(num * float64(scale))
	}

	if neg {
		total = -total
	}
	return total, nil
}
