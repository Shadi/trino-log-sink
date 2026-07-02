package querycli

import (
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestParseWithID(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantID  string
		wantErr bool
	}{
		{"id only", []string{"abc"}, "abc", false},
		{"id then flags", []string{"abc", "--url", "http://x"}, "abc", false},
		{"flags then id", []string{"--url", "http://x", "abc"}, "abc", false},
		{"flags both sides", []string{"--url", "http://x", "abc", "--timeout", "5s"}, "abc", false},
		{"no id", []string{"--url", "http://x"}, "", true},
		{"empty", nil, "", true},
		{"extra positional", []string{"a", "b"}, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			registerCommon(fs)
			id, err := parseWithID(fs, tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got id=%q", id)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if id != tc.wantID {
				t.Errorf("id = %q, want %q", id, tc.wantID)
			}
		})
	}
}

func TestMainListSuccess(t *testing.T) {
	var gotPath string
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.Query()
		io.WriteString(w, `{"queries":[{"queryId":"q-1"}],"count":1,"hasNext":false}`)
	}))
	defer srv.Close()

	code := Main([]string{"list", "--url", srv.URL, "--sort", "cpu", "--limit", "3", "--user", "alice"}, io.Discard)
	if code != exitOK {
		t.Fatalf("exit = %d, want 0", code)
	}
	if gotPath != "/api/v1/queries" {
		t.Errorf("path = %q", gotPath)
	}
	if gotQuery.Get("sort") != "cpu" || gotQuery.Get("limit") != "3" || gotQuery.Get("user") != "alice" || gotQuery.Get("dir") != "desc" || gotQuery.Get("range") != "24h" {
		t.Errorf("query = %v", gotQuery)
	}
}

func TestMainListClampsLimit(t *testing.T) {
	cases := map[string]string{"99999": "500", "-5": "1", "0": "1"}
	for in, want := range cases {
		var gotLimit string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotLimit = r.URL.Query().Get("limit")
			io.WriteString(w, `{"queries":[],"count":0,"hasNext":false}`)
		}))
		code := Main([]string{"list", "--url", srv.URL, "--limit", in}, io.Discard)
		srv.Close()
		if code != exitOK {
			t.Fatalf("--limit %s: exit = %d", in, code)
		}
		if gotLimit != want {
			t.Errorf("--limit %s → server saw limit=%q, want %q", in, gotLimit, want)
		}
	}
}

func TestMainGetEscapesIDAndSucceeds(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		io.WriteString(w, `{"queryId":"a b","inputs":[]}`)
	}))
	defer srv.Close()

	code := Main([]string{"get", "a b", "--url", srv.URL}, io.Discard)
	if code != exitOK {
		t.Fatalf("exit = %d, want 0", code)
	}
	if gotPath != "/api/v1/queries/a%20b" {
		t.Errorf("escaped path = %q, want /api/v1/queries/a%%20b", gotPath)
	}
}

func TestMainGetNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"query not found"}`, http.StatusNotFound)
	}))
	defer srv.Close()

	code := Main([]string{"get", "missing", "--url", srv.URL}, io.Discard)
	if code != exitNotFound {
		t.Fatalf("exit = %d, want %d", code, exitNotFound)
	}
}

func TestMainListServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	code := Main([]string{"list", "--url", srv.URL}, io.Discard)
	if code != exitError {
		t.Fatalf("exit = %d, want 1", code)
	}
}

func TestMainPlanSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"queryId":"q-1","plan":"└─ Window[k]\n   CPU: 1s (50.0%), Output: 10 rows","inputs":[]}`)
	}))
	defer srv.Close()

	code := Main([]string{"plan", "q-1", "--url", srv.URL, "--top", "5"}, io.Discard)
	if code != exitOK {
		t.Fatalf("exit = %d, want 0", code)
	}
}

func TestMainMissingURL(t *testing.T) {
	if code := Main([]string{"list"}, io.Discard); code != exitError {
		t.Fatalf("exit = %d, want 1", code)
	}
}

func TestMainUnknownSubcommand(t *testing.T) {
	if code := Main([]string{"bogus"}, io.Discard); code != exitError {
		t.Fatalf("exit = %d, want 1", code)
	}
}

func TestMainInvalidOutput(t *testing.T) {
	if code := Main([]string{"list", "--url", "http://x", "-o", "yaml"}, io.Discard); code != exitError {
		t.Fatalf("exit = %d, want 1", code)
	}
}

func TestMainListRejectsExtraArg(t *testing.T) {
	if code := Main([]string{"list", "--url", "http://x", "stray"}, io.Discard); code != exitError {
		t.Fatalf("exit = %d, want 1", code)
	}
}

func TestMainBadFlagExits1(t *testing.T) {
	if code := Main([]string{"list", "--url", "http://x", "--nope"}, io.Discard); code != exitError {
		t.Fatalf("exit = %d, want 1", code)
	}
}
