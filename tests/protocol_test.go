package tests

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	e := newEnv(t, 0)
	resp, err := http.Get(e.srv.URL + "/version")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(string(body), "sqld 0.20.0 ") {
		t.Fatalf("got %d %q", resp.StatusCode, body)
	}
}

func TestUnknownRoutes(t *testing.T) {
	e := newEnv(t, 0)
	for _, req := range []struct{ method, path string }{
		{http.MethodGet, "/"}, {http.MethodPost, "/v3-protobuf/pipeline"}, {http.MethodGet, "/v3/pipeline"}, {http.MethodOptions, "/"},
	} {
		r, _ := http.NewRequest(req.method, e.srv.URL+req.path, nil)
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s %s: got %d", req.method, req.path, resp.StatusCode)
		}
		// No CORS: a web page must not be able to read or drive the database.
		if v := resp.Header.Get("Access-Control-Allow-Origin"); v != "" {
			t.Errorf("%s %s: unexpected CORS header %q", req.method, req.path, v)
		}
	}
}

func TestInvalidBodyIsRejectedWithoutSession(t *testing.T) {
	e := newEnv(t, 0)
	resp, err := http.Post(e.srv.URL, "application/json", strings.NewReader(`{"statements": [`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("error body is not JSON: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest || body["error"] == "" {
		t.Fatalf("got %d %v", resp.StatusCode, body)
	}
	if len(resp.Cookies()) != 0 {
		t.Fatalf("a rejected request must not open a session: %v", resp.Cookies())
	}
}

func TestQueriesAndPositionalParams(t *testing.T) {
	c := newEnv(t, 0).client(true)
	r := c.mustExec(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"INSERT INTO users (name) VALUES ('Lucas')",
		map[string]any{"q": "SELECT * FROM users WHERE name = ?", "params": []any{"Lucas"}},
	)
	if r[1].RowsWritten != 1 {
		t.Errorf("insert rows_written = %d", r[1].RowsWritten)
	}
	sel := r[2]
	if strings.Join(sel.Columns, ",") != "id,name" || len(sel.Rows) != 1 || sel.Rows[0][1] != "Lucas" || sel.RowsRead != 1 {
		t.Errorf("unexpected select: %+v", sel)
	}
}

func TestNamedParamsAcceptAnyPrefix(t *testing.T) {
	c := newEnv(t, 0).client(false)
	r := c.mustExec(map[string]any{
		"q":      "SELECT :a, @b, $c",
		"params": map[string]any{":a": 10, "b": 20, "$c": 30},
	})[0]
	if r.Rows[0][0] != 10.0 || r.Rows[0][1] != 20.0 || r.Rows[0][2] != 30.0 {
		t.Errorf("got %v", r.Rows[0])
	}
}

func TestBatchStopsAtFirstError(t *testing.T) {
	c := newEnv(t, 0).client(false)
	_, body := c.post(`{"statements": ["SELECT 1", "SELECT * FROM missing_table", "SELECT 2"]}`)
	var raw []json.RawMessage
	if err := json.Unmarshal([]byte(body), &raw); err != nil || len(raw) != 3 {
		t.Fatalf("got %s (%v)", body, err)
	}
	if !strings.Contains(string(raw[1]), "no such table") {
		t.Errorf("statement 2: %s", raw[1])
	}
	if string(raw[2]) != "null" {
		t.Errorf("statement after the error must be null, got %s", raw[2])
	}
}

// rows_written used to come from guessing the statement type, so the word
// "returning" anywhere in the SQL turned an INSERT into a query with 0 writes.
func TestRowsWrittenIsExactForEveryStatementKind(t *testing.T) {
	c := newEnv(t, 0).client(true)
	c.mustExec("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)")
	for _, tc := range []struct {
		sql      string
		written  int64
		wantRows int
	}{
		{"INSERT INTO t (note) VALUES ('not returning anything')", 1, 0},
		{"INSERT INTO t (note) VALUES ('a'), ('b')", 2, 0},
		{"INSERT INTO t (note) VALUES ('c') RETURNING id, note", 1, 1},
		{"UPDATE t SET note = 'x'", 4, 0},
		{"UPDATE t SET note = 'y' WHERE id <= 2 RETURNING id", 2, 2},
		{"CREATE TABLE u (x)", 0, 0}, // must not echo the previous statement's count
		{"-- leading comment\nSELECT * FROM t", 0, 4},
		{"/* block */ WITH x AS (SELECT 1) SELECT * FROM x", 0, 1},
		{"DELETE FROM t WHERE id = 4", 1, 0},
	} {
		r := c.mustExec(tc.sql)[0]
		if r.RowsWritten != tc.written || len(r.Rows) != tc.wantRows {
			t.Errorf("%q: rows_written=%d rows=%d, want %d and %d", tc.sql, r.RowsWritten, len(r.Rows), tc.written, tc.wantRows)
		}
	}
	if got := c.scalar("SELECT count(*) FROM t"); got != 3.0 {
		t.Errorf("count = %v", got)
	}
}

// JSON numbers used to decode as float64: big ids lost precision and integer
// params were bound as REAL.
func TestNumericParamsKeepTheirType(t *testing.T) {
	c := newEnv(t, 0).client(false)
	_, body := c.post(`{"statements": [{"q": "SELECT ?, typeof(?), typeof(?)", "params": [9007199254740993, 7, 1.5]}]}`)
	if !strings.Contains(body, `[[9007199254740993,"integer","real"]]`) {
		t.Fatalf("got %s", body)
	}
}

func TestValuesWithoutDirectJSONForm(t *testing.T) {
	c := newEnv(t, 0).client(false)
	_, body := c.post(`{"statements": ["SELECT x'00ff', x'6869', x'', 'hi', 1e999, NULL"]}`)
	// Every BLOB uses sqld's {"base64"} form, even valid UTF-8, so it stays distinguishable from TEXT.
	// Inf has no JSON form and used to break the whole response.
	if !strings.Contains(body, `[[{"base64":"AP8="},{"base64":"aGk="},{"base64":""},"hi",null,null]]`) {
		t.Fatalf("got %s", body)
	}
}

// The driver parses DATETIME columns into time.Time; they used to come back
// as RFC 3339, so editing a row in DBeaver rewrote the stored format.
func TestDatetimeColumnsKeepSQLiteFormat(t *testing.T) {
	c := newEnv(t, 0).client(true)
	c.mustExec(
		"CREATE TABLE e (at DATETIME)",
		"INSERT INTO e VALUES ('2024-01-02 03:04:05'), ('2024-01-02 03:04:05.5')",
	)
	r := c.mustExec("SELECT at FROM e ORDER BY rowid")[0]
	if r.Rows[0][0] != "2024-01-02 03:04:05" || r.Rows[1][0] != "2024-01-02 03:04:05.5" {
		t.Fatalf("got %v", r.Rows)
	}
}

func TestBlobParamsRoundTrip(t *testing.T) {
	c := newEnv(t, 0).client(false)
	_, body := c.post(`{"statements": [{"q": "SELECT typeof(?1), hex(?1), ?1", "params": [{"base64": "AP8="}]}]}`)
	if !strings.Contains(body, `[["blob","00FF",{"base64":"AP8="}]]`) {
		t.Fatalf("got %s", body)
	}
}

func TestInvalidParamsRejectTheWholeRequest(t *testing.T) {
	e := newEnv(t, 0)
	c := e.client(false)
	c.mustExec("CREATE TABLE t (a, b)")
	for _, params := range []string{
		`{":x": 1, "@x": 2}`, // both would bind to "x"
		`[{"base64": "not base64!"}]`,
		`[{"foo": 1}]`,
		`[[1, 2]]`,
		`"scalar"`,
	} {
		status, body := c.post(`{"statements": ["INSERT INTO t VALUES (1, 1)", {"q": "INSERT INTO t VALUES (?, ?)", "params": ` + params + `}]}`)
		if status != http.StatusBadRequest || !strings.Contains(body, "params") && !strings.Contains(body, "param") {
			t.Errorf("params %s: got %d %s", params, status, body)
		}
	}
	if got := c.scalar("SELECT count(*) FROM t"); got != 0.0 {
		t.Fatalf("a rejected request must run nothing, count = %v", got)
	}
}

// Without CORS a page cannot read responses, but it can still fire a
// preflight-free text/plain POST and modify the database.
func TestCrossOriginBrowserRequestsAreRejected(t *testing.T) {
	e := newEnv(t, 0)
	for name, headers := range map[string]map[string]string{
		"sec-fetch-site": {"Sec-Fetch-Site": "cross-site"},
		"origin only":    {"Origin": "http://evil.example"},
	} {
		req, _ := http.NewRequest(http.MethodPost, e.srv.URL, strings.NewReader(`{"statements": ["CREATE TABLE pwned (x)"]}`))
		req.Header.Set("Content-Type", "text/plain")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s: got %d", name, resp.StatusCode)
		}
	}
	if got := e.client(false).scalar("SELECT count(*) FROM sqlite_master WHERE name = 'pwned'"); got != 0.0 {
		t.Fatal("cross-origin request ran")
	}
}
