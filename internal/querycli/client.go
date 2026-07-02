package querycli

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type commonFlags struct {
	url         string
	headers     headerList
	tokenFile   string
	tokenHeader string
	timeout     time.Duration
	insecure    bool
	output      string
}

type headerList []string

func (h *headerList) String() string { return strings.Join(*h, ", ") }

func (h *headerList) Set(v string) error {
	if !strings.Contains(v, ":") {
		return fmt.Errorf("header must be in \"Key: Value\" form: %q", v)
	}
	*h = append(*h, v)
	return nil
}

func registerCommon(fs *flag.FlagSet) *commonFlags {
	c := &commonFlags{}
	fs.StringVar(&c.url, "url", "", "base server URL (required)")
	fs.Var(&c.headers, "header", "extra request header \"Key: Value\" (repeatable)")
	fs.Var(&c.headers, "H", "shorthand for --header")
	fs.StringVar(&c.tokenFile, "token-file", "", "path to a file holding a token")
	fs.StringVar(&c.tokenHeader, "token-header", "", "override Authorization: Bearer — send the --token-file token raw in this header")
	fs.DurationVar(&c.timeout, "timeout", 30*time.Second, "HTTP timeout")
	fs.BoolVar(&c.insecure, "insecure", false, "skip TLS certificate verification")
	fs.StringVar(&c.output, "output", "table", "output format: table|json")
	fs.StringVar(&c.output, "o", "table", "shorthand for --output")
	return c
}

type client struct {
	base    string
	headers http.Header
	http    *http.Client
}

func newClient(c *commonFlags) (*client, error) {
	if strings.TrimSpace(c.url) == "" {
		return nil, fmt.Errorf("--url is required")
	}
	switch c.output {
	case "table", "json":
	default:
		return nil, fmt.Errorf("--output must be \"table\" or \"json\", got %q", c.output)
	}
	if c.tokenHeader != "" && c.tokenFile == "" {
		return nil, fmt.Errorf("--token-header requires --token-file")
	}

	h := http.Header{}
	for _, raw := range c.headers {
		k, v, _ := strings.Cut(raw, ":")
		h.Add(strings.TrimSpace(k), strings.TrimSpace(v))
	}
	if c.tokenFile != "" {
		b, err := os.ReadFile(c.tokenFile)
		if err != nil {
			return nil, fmt.Errorf("read token file: %w", err)
		}
		tok := strings.TrimSpace(string(b))
		if tok == "" {
			return nil, fmt.Errorf("token file %s is empty", c.tokenFile)
		}
		if c.tokenHeader != "" {
			h.Set(c.tokenHeader, tok)
		} else {
			h.Set("Authorization", "Bearer "+tok)
		}
	}

	tr := &http.Transport{}
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		tr = base.Clone()
	}
	if c.insecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &client{
		base:    strings.TrimRight(c.url, "/"),
		headers: h,
		http: &http.Client{
			Timeout:   c.timeout,
			Transport: tr,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *client) getQuery(id string) (queryDetail, error) {
	var d queryDetail
	err := c.getJSON("/api/v1/queries/"+url.PathEscape(id), nil, &d)
	return d, err
}

func (c *client) getJSON(path string, q url.Values, dst any) error {
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	for k, vs := range c.headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}
	if resp.StatusCode/100 != 2 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		msg := strings.TrimSpace(string(snippet))
		if msg == "" {
			return fmt.Errorf("server returned %s", resp.Status)
		}
		return fmt.Errorf("server returned %s: %s", resp.Status, msg)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(dst)
}
