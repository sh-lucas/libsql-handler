package tests

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

// Hrana over HTTP, as spoken by the official libSQL clients.

type hValue struct {
	Type   string          `json:"type"`
	Value  json.RawMessage `json:"value,omitempty"`
	Base64 *string         `json:"base64,omitempty"`
}

type hCol struct {
	Name     *string `json:"name"`
	Decltype *string `json:"decltype"`
}

type hStmtResult struct {
	Cols             []hCol     `json:"cols"`
	Rows             [][]hValue `json:"rows"`
	AffectedRowCount uint64     `json:"affected_row_count"`
	LastInsertRowid  *string    `json:"last_insert_rowid"`
	RowsRead         uint64     `json:"rows_read"`
	RowsWritten      uint64     `json:"rows_written"`
}

type hError struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

type hBatchResult struct {
	StepResults []*hStmtResult `json:"step_results"`
	StepErrors  []*hError      `json:"step_errors"`
}

type hDescribe struct {
	Params     []struct{ Name *string } `json:"params"`
	Cols       []hCol                   `json:"cols"`
	IsExplain  bool                     `json:"is_explain"`
	IsReadonly bool                     `json:"is_readonly"`
}

type hResponse struct {
	Type         string          `json:"type"`
	Result       json.RawMessage `json:"result"`
	IsAutocommit *bool           `json:"is_autocommit"`
}

type hResult struct {
	Type     string     `json:"type"`
	Response *hResponse `json:"response"`
	Error    *hError    `json:"error"`
}

type hPipelineResp struct {
	Baton   *string   `json:"baton"`
	BaseURL *string   `json:"base_url"`
	Results []hResult `json:"results"`
}

// val converts a Go value into a Hrana value.
func val(v any) map[string]any {
	switch x := v.(type) {
	case nil:
		return map[string]any{"type": "null"}
	case int:
		return map[string]any{"type": "integer", "value": strconv.Itoa(x)}
	case int64:
		return map[string]any{"type": "integer", "value": strconv.FormatInt(x, 10)}
	case float64:
		return map[string]any{"type": "float", "value": x}
	case string:
		return map[string]any{"type": "text", "value": x}
	case []byte:
		return map[string]any{"type": "blob", "base64": base64.StdEncoding.EncodeToString(x)}
	}
	panic("unsupported value")
}

func vals(vs ...any) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = val(v)
	}
	return out
}

// stmt builds a Stmt with positional args.
func stmt(sql string, args ...any) map[string]any {
	s := map[string]any{"sql": sql}
	if len(args) > 0 {
		s["args"] = vals(args...)
	}
	return s
}

func execute(s map[string]any) map[string]any {
	return map[string]any{"type": "execute", "stmt": s}
}

func execSQL(sql string, args ...any) map[string]any { return execute(stmt(sql, args...)) }

func batchReq(steps ...map[string]any) map[string]any {
	return map[string]any{"type": "batch", "batch": map[string]any{"steps": steps}}
}

func step(s map[string]any, cond map[string]any) map[string]any {
	st := map[string]any{"stmt": s}
	if cond != nil {
		st["condition"] = cond
	}
	return st
}

var closeReq = map[string]any{"type": "close"}

// postJSON sends body to path and returns status and raw response.
func postJSON(t testing.TB, url, path string, body any) (int, string) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(raw)
}

// hstream is a Hrana stream as a client sees it: a baton that changes with
// every response.
type hstream struct {
	t     testing.TB
	url   string
	path  string
	baton *string
}

func (e *env) stream() *hstream { return &hstream{t: e.t, url: e.srv.URL, path: "/v3/pipeline"} }

// raw runs a pipeline and returns the HTTP status and body without checks.
func (s *hstream) raw(reqs ...map[string]any) (int, string) {
	s.t.Helper()
	return postJSON(s.t, s.url, s.path, map[string]any{"baton": s.baton, "requests": reqs})
}

// run runs a pipeline that must succeed at the HTTP level and returns one
// result per request.
func (s *hstream) run(reqs ...map[string]any) []hResult {
	s.t.Helper()
	status, body := s.raw(reqs...)
	if status != http.StatusOK {
		s.t.Fatalf("pipeline: status %d: %s", status, body)
	}
	var resp hPipelineResp
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		s.t.Fatalf("decoding %s: %v", body, err)
	}
	if len(resp.Results) != len(reqs) {
		s.t.Fatalf("got %d results for %d requests: %s", len(resp.Results), len(reqs), body)
	}
	if resp.BaseURL != nil {
		s.t.Fatalf("unexpected base_url %q", *resp.BaseURL)
	}
	if s.baton != nil && resp.Baton != nil && *resp.Baton == *s.baton {
		s.t.Fatalf("baton was not rotated: %q", *s.baton)
	}
	s.baton = resp.Baton
	for i, r := range resp.Results {
		if r.Type == "ok" && (r.Response == nil || r.Response.Type != reqs[i]["type"]) {
			s.t.Fatalf("result %d: response type %+v does not match request %v", i, r.Response, reqs[i]["type"])
		}
	}
	return resp.Results
}

// ok returns the response of a successful result.
func ok(t testing.TB, r hResult) *hResponse {
	t.Helper()
	if r.Type != "ok" {
		t.Fatalf("want ok, got error %+v", r.Error)
	}
	return r.Response
}

// failed returns the error of a failed result.
func failed(t testing.TB, r hResult) *hError {
	t.Helper()
	if r.Type != "error" || r.Error == nil || r.Error.Message == "" {
		t.Fatalf("want error, got %+v", r)
	}
	return r.Error
}

func execResult(t testing.TB, r hResult) *hStmtResult {
	t.Helper()
	var out hStmtResult
	if err := json.Unmarshal(ok(t, r).Result, &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

func batchResult(t testing.TB, r hResult) *hBatchResult {
	t.Helper()
	var out hBatchResult
	if err := json.Unmarshal(ok(t, r).Result, &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

// exec runs one statement on the stream and returns its result.
func (s *hstream) exec(sql string, args ...any) *hStmtResult {
	s.t.Helper()
	return execResult(s.t, s.run(execSQL(sql, args...))[0])
}

// scalar runs a query on the stream and returns its single value.
func (s *hstream) scalar(sql string, args ...any) hValue {
	s.t.Helper()
	r := s.exec(sql, args...)
	if len(r.Rows) != 1 || len(r.Rows[0]) != 1 {
		s.t.Fatalf("%q: want one value, got %+v", sql, r.Rows)
	}
	return r.Rows[0][0]
}

// str renders a value compactly for comparisons, e.g. integer:42, text:"x".
func (v hValue) String() string {
	switch v.Type {
	case "null":
		return "null"
	case "blob":
		if v.Base64 == nil {
			return "blob:<missing>"
		}
		return "blob:" + *v.Base64
	}
	return v.Type + ":" + string(v.Value)
}

func row(vs []hValue) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = v.String()
	}
	return strings.Join(parts, " ")
}

// cursor runs a batch on /v3/cursor and returns the header and entries.
func cursor(t testing.TB, url string, baton *string, steps ...map[string]any) (int, map[string]any, []map[string]any, string) {
	t.Helper()
	status, body := postJSON(t, url, "/v3/cursor", map[string]any{"baton": baton, "batch": map[string]any{"steps": steps}})
	if status != http.StatusOK {
		return status, nil, nil, body
	}
	sc := bufio.NewScanner(strings.NewReader(body))
	sc.Buffer(nil, 1<<24)
	var lines []map[string]any
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("cursor line %q: %v", sc.Text(), err)
		}
		lines = append(lines, m)
	}
	if len(lines) == 0 {
		t.Fatalf("empty cursor response")
	}
	return status, lines[0], lines[1:], body
}

func ptr[T any](v T) *T { return &v }
