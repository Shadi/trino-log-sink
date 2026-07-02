package querycli

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGetJSONSendsTokenAndHeaders(t *testing.T) {
	var gotAuth, gotDebug, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotDebug = r.Header.Get("X-Debug")
		gotAccept = r.Header.Get("Accept")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	tf := filepath.Join(t.TempDir(), "tok")
	if err := os.WriteFile(tf, []byte("  s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	c := &commonFlags{url: srv.URL, tokenFile: tf, timeout: 5 * time.Second, output: "table"}
	if err := c.headers.Set("X-Debug: 1"); err != nil {
		t.Fatal(err)
	}
	cl, err := newClient(c)
	if err != nil {
		t.Fatal(err)
	}
	var dst struct{ OK bool }
	if err := cl.getJSON("/x", nil, &dst); err != nil {
		t.Fatalf("getJSON: %v", err)
	}
	if !dst.OK {
		t.Errorf("decode failed, dst=%+v", dst)
	}
	if gotAuth != "Bearer s3cr3t" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer s3cr3t")
	}
	if gotDebug != "1" {
		t.Errorf("X-Debug = %q, want 1", gotDebug)
	}
	if gotAccept != "application/json" {
		t.Errorf("Accept = %q", gotAccept)
	}
}

func TestGetJSONTokenHeaderOverride(t *testing.T) {
	var gotAuth, gotCustom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCustom = r.Header.Get("X-Api-Key")
		io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	tf := filepath.Join(t.TempDir(), "tok")
	if err := os.WriteFile(tf, []byte("abc123"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := &commonFlags{url: srv.URL, tokenFile: tf, tokenHeader: "X-Api-Key", timeout: 5 * time.Second, output: "table"}
	cl, err := newClient(c)
	if err != nil {
		t.Fatal(err)
	}
	var dst map[string]any
	if err := cl.getJSON("/x", nil, &dst); err != nil {
		t.Fatal(err)
	}
	if gotCustom != "abc123" {
		t.Errorf("X-Api-Key = %q, want abc123", gotCustom)
	}
	if gotAuth != "" {
		t.Errorf("Authorization should be empty with token-header override, got %q", gotAuth)
	}
}

func TestGetJSONNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"query not found: x"}`, http.StatusNotFound)
	}))
	defer srv.Close()

	cl, err := newClient(&commonFlags{url: srv.URL, timeout: 5 * time.Second, output: "table"})
	if err != nil {
		t.Fatal(err)
	}
	var dst map[string]any
	err = cl.getJSON("/x", nil, &dst)
	if !errors.Is(err, errNotFound) {
		t.Fatalf("err = %v, want errNotFound", err)
	}
}

func TestGetJSONNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	cl, err := newClient(&commonFlags{url: srv.URL, timeout: 5 * time.Second, output: "table"})
	if err != nil {
		t.Fatal(err)
	}
	var dst map[string]any
	err = cl.getJSON("/x", nil, &dst)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("err = %v, want one mentioning 500", err)
	}
}

func TestNewClientRequiresURL(t *testing.T) {
	if _, err := newClient(&commonFlags{}); err == nil {
		t.Fatal("expected error for empty --url")
	}
}

func TestNewClientMissingTokenFile(t *testing.T) {
	_, err := newClient(&commonFlags{url: "http://x", tokenFile: "/no/such/file", output: "table"})
	if err == nil {
		t.Fatal("expected error for missing token file")
	}
}

func TestNewClientEmptyTokenFile(t *testing.T) {
	tf := filepath.Join(t.TempDir(), "tok")
	if err := os.WriteFile(tf, []byte("   \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := newClient(&commonFlags{url: "http://x", tokenFile: tf, output: "table"})
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("expected empty-token error, got %v", err)
	}
}

func TestNewClientTokenHeaderRequiresFile(t *testing.T) {
	_, err := newClient(&commonFlags{url: "http://x", tokenHeader: "X-Api-Key", output: "table"})
	if err == nil || !strings.Contains(err.Error(), "token-header requires") {
		t.Fatalf("expected token-header-requires-file error, got %v", err)
	}
}

func TestGetJSONRefusesRedirect(t *testing.T) {
	var targetHit bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHit = true
		io.WriteString(w, `{"ok":true}`)
	}))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/x", http.StatusFound)
	}))
	defer srv.Close()

	cl, err := newClient(&commonFlags{url: srv.URL, timeout: 5 * time.Second, output: "table"})
	if err != nil {
		t.Fatal(err)
	}
	var dst map[string]any
	if err := cl.getJSON("/x", nil, &dst); err == nil {
		t.Fatal("expected redirect to be refused, got nil error")
	}
	if targetHit {
		t.Error("redirect target was contacted; headers/token could leak")
	}
}

func TestHeaderListSetRejectsMalformed(t *testing.T) {
	var h headerList
	if err := h.Set("no-colon"); err == nil {
		t.Fatal("expected error for header without colon")
	}
	if err := h.Set("K: V"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
