// Package tests exercises the handler end to end over HTTP, through its public
// API only, with the same JSON the DBeaver driver sends.
package tests

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	libsqlhandler "github.com/sh-lucas/libsql-handler"

	_ "modernc.org/sqlite"
)

type env struct {
	t   testing.TB
	db  *sql.DB
	h   *libsqlhandler.LibSQLHandler
	srv *httptest.Server
}

// newEnv serves a fresh file database (file, not :memory:, so every
// connection sees the same data). maxOpen <= 0 leaves the pool unbounded.
func newEnv(t testing.TB, maxOpen int, opts ...libsqlhandler.Option) *env {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	if maxOpen > 0 {
		db.SetMaxOpenConns(maxOpen)
	}
	h := libsqlhandler.New(db, opts...)
	srv := httptest.NewServer(h)
	t.Cleanup(func() {
		srv.Close()
		h.Close()
		db.Close()
	})
	return &env{t: t, db: db, h: h, srv: srv}
}

type result struct {
	Columns     []string `json:"columns"`
	Rows        [][]any  `json:"rows"`
	RowsRead    int64    `json:"rows_read"`
	RowsWritten int64    `json:"rows_written"`
}

type stmtResult struct {
	Results *result `json:"results"`
	Error   string  `json:"error"`
}

type client struct {
	env  *env
	http *http.Client
}

// sticky clients keep cookies like DBeaver's CookieManager; others behave like curl.
func (e *env) client(sticky bool) *client {
	c := &http.Client{Timeout: 10 * time.Second}
	if sticky {
		c.Jar, _ = cookiejar.New(nil)
	}
	return &client{env: e, http: c}
}

// post sends a raw body and returns status and body.
func (c *client) post(body string) (int, string) {
	c.env.t.Helper()
	resp, err := c.http.Post(c.env.srv.URL, "application/json", strings.NewReader(body))
	if err != nil {
		c.env.t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		c.env.t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

// exec sends the statements (strings or {"q","params"} maps) and returns one
// entry per statement; nil entries were skipped after an earlier error.
func (c *client) exec(stmts ...any) []*stmtResult {
	c.env.t.Helper()
	body, err := json.Marshal(map[string]any{"statements": stmts})
	if err != nil {
		c.env.t.Fatal(err)
	}
	status, resp := c.post(string(body))
	if status != http.StatusOK {
		c.env.t.Fatalf("status %d: %s", status, resp)
	}
	var out []*stmtResult
	if err := json.Unmarshal([]byte(resp), &out); err != nil {
		c.env.t.Fatalf("decoding %q: %v", resp, err)
	}
	if len(out) != len(stmts) {
		c.env.t.Fatalf("got %d results for %d statements", len(out), len(stmts))
	}
	return out
}

// mustExec is exec that fails the test on any statement error.
func (c *client) mustExec(stmts ...any) []*result {
	c.env.t.Helper()
	var out []*result
	for i, r := range c.exec(stmts...) {
		if r == nil || r.Error != "" {
			c.env.t.Fatalf("statement %d (%v) failed: %+v", i, stmts[i], r)
		}
		out = append(out, r.Results)
	}
	return out
}

// scalar runs one query and returns the first column of its single row.
func (c *client) scalar(query string) any {
	c.env.t.Helper()
	r := c.mustExec(query)[0]
	if len(r.Rows) != 1 {
		c.env.t.Fatalf("%q: want 1 row, got %v", query, r.Rows)
	}
	return r.Rows[0][0]
}
