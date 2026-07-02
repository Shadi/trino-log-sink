package querycli

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

func humanMS(ms int64) string {
	if ms <= 0 {
		return "0"
	}
	d := time.Duration(ms) * time.Millisecond
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%dh%dm", d/time.Hour, (d%time.Hour)/time.Minute)
	case d >= time.Minute:
		return fmt.Sprintf("%dm%ds", d/time.Minute, (d%time.Minute)/time.Second)
	case d >= time.Second:
		return fmt.Sprintf("%.1fs", d.Seconds())
	default:
		return fmt.Sprintf("%dms", ms)
	}
}

func humanBytes(n int64) string {
	if n <= 0 {
		return "0"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	f := float64(n)
	i := -1
	for f >= unit && i < len(units)-1 {
		f /= unit
		i++
	}
	return fmt.Sprintf("%.1f%s", f, units[i])
}

func humanCount(n int64) string {
	neg := n < 0
	s := strconv.FormatInt(n, 10)
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteByte(s[i])
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

func humanBytesPtr(p *int64) string {
	if p == nil {
		return "-"
	}
	return humanBytes(*p)
}

func humanCountPtr(p *int64) string {
	if p == nil {
		return "-"
	}
	return humanCount(*p)
}

func trunc(s string, n int) string {
	if n < 0 {
		n = 0
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n == 0 {
		return ""
	}
	if n == 1 {
		return string(r[0])
	}
	return string(r[:n-1]) + "…"
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func render(out io.Writer, format string, jsonVal any, table func(io.Writer)) error {
	if format == "json" {
		return printJSON(out, jsonVal)
	}
	table(out)
	return nil
}
