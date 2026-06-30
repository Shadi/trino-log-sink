package server

import (
	"fmt"
	"html/template"
	"strconv"
	"strings"
	"time"
)

const tsLayout = "2006-01-02 15:04:05"

func humanBytes(n int64) string {
	if n <= 0 {
		return "0"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func humanBytesPtr(n *int64) string {
	if n == nil {
		return "—"
	}
	return humanBytes(*n)
}

func humanMillis(ms int64) string {
	if ms <= 0 {
		return "0"
	}
	d := time.Duration(ms) * time.Millisecond
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", ms)
	case d < time.Minute:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d < time.Hour:
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

func humanNum(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

func humanNumPtr(n *int64) string {
	if n == nil {
		return "—"
	}
	return humanNum(*n)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.UTC().Format(tsLayout)
}

func formatTimeOpt(t *time.Time) string {
	if t == nil {
		return "—"
	}
	return formatTime(*t)
}

func formatStartTime(start *time.Time, create time.Time) string {
	if start != nil {
		return formatTime(*start)
	}
	return formatTime(create)
}

func stateBadge(state string) template.HTML {
	cls := "other"
	if state == "FINISHED" || state == "FAILED" {
		cls = state
	}
	return template.HTML(fmt.Sprintf(`<span class="badge %s">%s</span>`,
		template.HTMLEscapeString(cls), template.HTMLEscapeString(state)))
}
