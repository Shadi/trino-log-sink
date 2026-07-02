package querycli

import "testing"

func TestHumanMS(t *testing.T) {
	cases := map[int64]string{
		0:        "0",
		-5:       "0",
		42:       "42ms",
		1500:     "1.5s",
		65000:    "1m5s",
		59929133: "16h38m",
	}
	for in, want := range cases {
		if got := humanMS(in); got != want {
			t.Errorf("humanMS(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	cases := map[int64]string{
		0:            "0",
		512:          "512B",
		2048:         "2.0KiB",
		16658425623:  "15.5GiB",
		314417094561: "292.8GiB",
	}
	for in, want := range cases {
		if got := humanBytes(in); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestHumanCount(t *testing.T) {
	cases := map[int64]string{
		0:         "0",
		42:        "42",
		1000:      "1,000",
		846303721: "846,303,721",
		-1234:     "-1,234",
	}
	for in, want := range cases {
		if got := humanCount(in); got != want {
			t.Errorf("humanCount(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestPtrHelpers(t *testing.T) {
	if humanBytesPtr(nil) != "-" || humanCountPtr(nil) != "-" {
		t.Error("nil pointers should render as -")
	}
	n := int64(2048)
	if humanBytesPtr(&n) != "2.0KiB" {
		t.Errorf("humanBytesPtr = %q", humanBytesPtr(&n))
	}
}

func TestTrunc(t *testing.T) {
	if got := trunc("hello", 10); got != "hello" {
		t.Errorf("got %q", got)
	}
	if got := trunc("hello world", 5); got != "hell…" {
		t.Errorf("got %q, want hell…", got)
	}
}

func TestOneLine(t *testing.T) {
	if got := oneLine("a\n  b\t c"); got != "a b c" {
		t.Errorf("got %q", got)
	}
}
