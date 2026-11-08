package tests

import (
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"testing"
)

// Expectations here mirror what a real sqld (0.24) answers; the same
// pipelines are replayed against it in sqld_test.go.

func TestHranaVersionEndpoints(t *testing.T) {
	e := newEnv(t, 0)
	for path, want := range map[string]int{
		"/v2": http.StatusOK, "/v3": http.StatusOK,
		// The TS client prefers protobuf when offered; we only speak JSON.
		"/v3-protobuf": http.StatusNotFound,
	} {
		resp, err := http.Get(e.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s: got %d, want %d", path, resp.StatusCode, want)
		}
	}
}

func TestHranaBothPipelineVersions(t *testing.T) {
	e := newEnv(t, 0)
	for _, path := range []string{"/v2/pipeline", "/v3/pipeline"} {
		s := e.stream()
		s.path = path
		if got := s.scalar("SELECT 42").String(); got != `integer:"42"` {
			t.Errorf("%s: got %s", path, got)
		}
	}
}

func TestHranaValuesRoundTrip(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	r := s.exec("SELECT ?, ?, ?, ?, ?, ?, ?", nil, int64(math.MaxInt64), int64(math.MinInt64), 1.5, "héllo\x00", []byte{0, 0xff, 1}, math.Copysign(0, -1))
	want := `null integer:"9223372036854775807" integer:"-9223372036854775808" float:1.5 text:"héllo\u0000" blob:AP8B float:-0`
	if got := row(r.Rows[0]); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	// Integers beyond 2^53 stay exact; sqld also accepts padded base64.
	if got := s.scalar("SELECT ? + 1", int64(1)<<53+1).String(); got != `integer:"9007199254740994"` {
		t.Fatalf("got %s", got)
	}
	if got := s.scalar("SELECT 1e999").String(); got != "float:null" {
		t.Fatalf("infinity: got %s", got)
	}
	var f float64
	if v := s.scalar("SELECT 9223372036854775807 + 1"); v.Type != "float" || json.Unmarshal(v.Value, &f) != nil || f != 1<<63 {
		t.Fatalf("overflow to float: got %s", v)
	}
	if got := s.scalar("SELECT typeof(?)", 2.0).String(); got != `text:"real"` {
		t.Fatalf("float param bound as %s", got)
	}
}

func TestHranaStatementResults(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	s.run(map[string]any{"type": "sequence", "sql": `
		CREATE TABLE a (id INTEGER PRIMARY KEY, v INT UNIQUE, d DATETIME);
		CREATE TABLE log (x);
		CREATE TRIGGER tr AFTER INSERT ON a BEGIN INSERT INTO log VALUES (new.v); END;`})

	type counters struct {
		affected uint64
		rowid    string // "" means null
	}
	check := func(sql string, want counters) *hStmtResult {
		t.Helper()
		r := s.exec(sql)
		got := counters{affected: r.AffectedRowCount}
		if r.LastInsertRowid != nil {
			got.rowid = *r.LastInsertRowid
		}
		if got != want {
			t.Errorf("%s: got %+v, want %+v", sql, got, want)
		}
		return r
	}
	// Trigger rows are not counted; last_insert_rowid is reported for every
	// INSERT, even one that inserted nothing, and only for INSERTs.
	check("INSERT INTO a (v) VALUES (10), (20)", counters{2, "2"})
	check("SELECT 1", counters{0, ""})
	check("UPDATE a SET v = v + 1", counters{2, ""})
	check("UPDATE a SET v = v + 1 WHERE 0", counters{0, ""})
	check("INSERT INTO a (v) SELECT 5 WHERE 0", counters{0, "2"})
	check("DELETE FROM a WHERE id = 1", counters{1, ""})
	check("REPLACE INTO a (id, v) VALUES (7, 99)", counters{1, "7"})
	check("WITH q AS (SELECT 1) INSERT INTO log SELECT * FROM q", counters{1, "4"})
	check("CREATE TABLE b (x)", counters{0, ""})
	r := check("INSERT INTO a (v) VALUES (1), (2) RETURNING id, v", counters{2, "9"})
	if got := row(r.Rows[0]) + " | " + row(r.Rows[1]); got != `integer:"8" integer:"1" | integer:"9" integer:"2"` {
		t.Errorf("RETURNING rows: %s", got)
	}

	r = s.exec("SELECT id, v AS vv, d, v + 1 FROM a WHERE id = 7")
	var cols []string
	for _, c := range r.Cols {
		decl := "null"
		if c.Decltype != nil {
			decl = strings.ToUpper(*c.Decltype)
		}
		cols = append(cols, *c.Name+":"+decl)
	}
	if got := strings.Join(cols, " "); got != "id:INTEGER vv:INT d:DATETIME v + 1:null" {
		t.Errorf("cols: %s", got)
	}

	// want_rows=false runs the statement but returns no rows.
	r = execResult(t, s.run(execute(map[string]any{"sql": "INSERT INTO b VALUES (1), (2) RETURNING x", "want_rows": false}))[0])
	if len(r.Rows) != 0 || len(r.Cols) != 1 || r.AffectedRowCount != 2 {
		t.Errorf("want_rows=false: %+v", r)
	}
	if got := s.scalar("SELECT count(*) FROM b").String(); got != `integer:"2"` {
		t.Errorf("want_rows=false must still execute: %s", got)
	}
}

func TestHranaParameters(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	named := func(sql string, args map[string]any) map[string]any {
		var na []any
		for k, v := range args {
			na = append(na, map[string]any{"name": k, "value": val(v)})
		}
		return execute(map[string]any{"sql": sql, "named_args": na})
	}

	for _, c := range []struct {
		name string
		req  map[string]any
		want string
	}{
		{"positional", execSQL("SELECT ?, ?", 1, "x"), `integer:"1" text:"x"`},
		{"numbered", execSQL("SELECT ?2, ?1, ?", 1, 2, 3), `integer:"2" integer:"1" integer:"3"`},
		{"named with prefix", named("SELECT :a, @b, $c", map[string]any{":a": 1, "@b": 2, "$c": 3}), `integer:"1" integer:"2" integer:"3"`},
		{"named guessed prefix", named("SELECT :a, @b, $c, :a", map[string]any{"a": 1, "b": 2, "c": 3}), `integer:"1" integer:"2" integer:"3" integer:"1"`},
		// Names database/sql cannot bind by name: shared bare name, leading digit.
		{"same name, other prefixes", named("SELECT :a, @a, $a, :1x, :a", map[string]any{":a": 1, "@a": 2, "$a": 3, ":1x": 4}), `integer:"1" integer:"2" integer:"3" integer:"4" integer:"1"`},
	} {
		r := execResult(t, s.run(c.req)[0])
		if got := row(r.Rows[0]); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
	// Column names keep the parameter names, even where the statement had
	// to be rewritten to bind them.
	r := execResult(t, s.run(named("SELECT @a, :a + 1, :1x", map[string]any{"@a": 1, ":a": 2, "1x": 3}))[0])
	var cols []string
	for _, c := range r.Cols {
		cols = append(cols, *c.Name)
	}
	if got := strings.Join(cols, ", "); got != "@a, :a + 1, :1x" {
		t.Errorf("column names: %s", got)
	}

	for _, c := range []struct {
		name string
		req  map[string]any
		code string
	}{
		{"missing positional", execSQL("SELECT ?, ?", 1), "ARGS_INVALID"},
		{"too many", execSQL("SELECT ?", 1, 2), "ARGS_INVALID"},
		{"args without params", execSQL("SELECT 1", 1), "ARGS_INVALID"},
		{"missing named", named("SELECT :a", map[string]any{"b": 1}), "ARGS_INVALID"},
		{"extra named", named("SELECT :a", map[string]any{"a": 1, "zz": 2}), "ARGS_INVALID"},
		{"positional for named", execSQL("SELECT :a", 1), "ARGS_INVALID"},
		{"named for positional", named("SELECT ?", map[string]any{"a": 1}), "ARGS_INVALID"},
		{"both kinds", execute(map[string]any{"sql": "SELECT :a, ?", "args": vals(1), "named_args": []any{map[string]any{"name": "a", "value": val(1)}}}), "ARGS_BOTH_POSITIONAL_AND_NAMED"},
	} {
		if got := failed(t, s.run(c.req)[0]); got.Code != c.code {
			t.Errorf("%s: got %+v, want code %s", c.name, got, c.code)
		}
	}
}

func TestHranaStatementErrors(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	res := s.run(
		execSQL("CREATE TABLE u (x UNIQUE)"),
		execSQL("INSERT INTO u VALUES (1), (1)"),
		execSQL("SELECT * FROM missing_table"),
		execSQL(""),
		execSQL("  -- only a comment"),
		execSQL("SELECT 1; SELECT 2"),
		execSQL("SELECT 1;"), // a trailing semicolon is still one statement
		execSQL("COMMIT"),
		execSQL("SELECT count(*) FROM u"), // later requests still run
	)
	for i, want := range map[int]string{
		1: "SQLITE_CONSTRAINT", 2: "SQLITE_UNKNOWN", 3: "SQL_NO_STATEMENT", 4: "SQL_NO_STATEMENT",
		5: "SQL_MANY_STATEMENTS", 7: "SQLITE_UNKNOWN",
	} {
		if got := failed(t, res[i]); got.Code != want {
			t.Errorf("request %d: got %+v, want code %s", i, got, want)
		}
	}
	if !strings.Contains(res[2].Error.Message, "no such table: missing_table") {
		t.Errorf("message: %q", res[2].Error.Message)
	}
	execResult(t, res[6])
	if got := row(execResult(t, res[8]).Rows[0]); got != `integer:"0"` {
		t.Errorf("failed INSERT must not leave rows: %s", got)
	}
}

func TestHranaBatchConditions(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	okc := func(i int) map[string]any { return map[string]any{"type": "ok", "step": i} }
	errc := func(i int) map[string]any { return map[string]any{"type": "error", "step": i} }
	not := func(c map[string]any) map[string]any { return map[string]any{"type": "not", "cond": c} }
	autocommit := map[string]any{"type": "is_autocommit"}

	b := batchResult(t, s.run(batchReq(
		step(stmt("BEGIN"), nil),                                                    // 0 runs
		step(stmt("SELECT * FROM missing"), okc(0)),                                 // 1 runs, fails
		step(stmt("SELECT 'skipped'"), okc(1)),                                      // 2 skipped
		step(stmt("SELECT 'ran'"), errc(1)),                                         // 3 runs
		step(stmt("SELECT 'not autocommit'"), not(autocommit)),                      // 4 runs: inside BEGIN
		step(stmt("SELECT 'and'"), map[string]any{"type": "and", "conds": []any{}}), // 5 runs: empty and is true
		step(stmt("SELECT 'or'"), map[string]any{"type": "or", "conds": []any{}}),   // 6 skipped: empty or is false
		step(stmt("SELECT 'skipped ok'"), okc(2)),                                   // 7 skipped: step 2 was skipped
		step(stmt("SELECT 'error of skipped'"), errc(2)),                            // 8 runs: sqld treats skipped as not ok
		step(stmt("ROLLBACK"), nil),                                                 // 9 runs
		step(stmt("SELECT 'autocommit'"), autocommit),                               // 10 runs
	))[0])

	if len(b.StepResults) != 11 || len(b.StepErrors) != 11 {
		t.Fatalf("want 11 entries, got %d/%d", len(b.StepResults), len(b.StepErrors))
	}
	ran := map[int]bool{0: true, 3: true, 4: true, 5: true, 8: true, 9: true, 10: true}
	for i := range 11 {
		switch {
		case i == 1:
			if b.StepResults[i] != nil || b.StepErrors[i] == nil || b.StepErrors[i].Code != "SQLITE_UNKNOWN" {
				t.Errorf("step 1: want error, got %+v %+v", b.StepResults[i], b.StepErrors[i])
			}
		case ran[i]:
			if b.StepResults[i] == nil || b.StepErrors[i] != nil {
				t.Errorf("step %d: want result, got %+v %+v", i, b.StepResults[i], b.StepErrors[i])
			}
		default:
			if b.StepResults[i] != nil || b.StepErrors[i] != nil {
				t.Errorf("step %d: want skipped, got %+v %+v", i, b.StepResults[i], b.StepErrors[i])
			}
		}
	}

	// A condition may only refer to an earlier step; anything else is a
	// protocol error that fails the whole request.
	for _, cond := range []map[string]any{okc(1), errc(5), not(okc(0))} {
		s := e.stream()
		status, body := s.raw(batchReq(step(stmt("SELECT 1"), cond), step(stmt("SELECT 2"), nil)))
		if status != http.StatusBadRequest {
			t.Errorf("cond %v: got %d %s", cond, status, body)
		}
	}
}

func TestHranaSequenceStoreSqlDescribe(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	res := s.run(
		map[string]any{"type": "sequence", "sql": "CREATE TABLE dd (id INTEGER PRIMARY KEY, d DateTime); INSERT INTO dd (d) VALUES ('x'); SELECT 5"},
		map[string]any{"type": "store_sql", "sql_id": 1, "sql": "SELECT :a AS x, ?2, @b, id, d FROM dd"},
		map[string]any{"type": "describe", "sql_id": 1},
		map[string]any{"type": "store_sql", "sql_id": 2, "sql": "SELECT :a, @b, id, d FROM dd"},
		execute(map[string]any{"sql_id": 2, "named_args": []any{
			map[string]any{"name": "a", "value": val(1)}, map[string]any{"name": "@b", "value": val(3)},
		}}),
		map[string]any{"type": "close_sql", "sql_id": 99}, // closing an unknown id is fine
		map[string]any{"type": "describe", "sql": "INSERT INTO dd (d) VALUES (?) RETURNING id"},
		map[string]any{"type": "describe", "sql": "EXPLAIN SELECT 1"},
		map[string]any{"type": "describe", "sql": "BEGIN"},
		map[string]any{"type": "describe", "sql": "SELECT ?5, ?, :z"},
		map[string]any{"type": "describe", "sql": "CREATE TABLE zz (a)"},
		map[string]any{"type": "describe", "sql": "SELECT * FROM nope"},
		map[string]any{"type": "describe", "sql": "WITH q AS (SELECT 1) SELECT * FROM q"},
		map[string]any{"type": "sequence", "sql": "SELECT * FROM nope"},
		map[string]any{"type": "get_autocommit"},
	)
	for _, i := range []int{0, 1, 3, 5} {
		ok(t, res[i])
	}
	describe := func(r hResult) string {
		t.Helper()
		var d hDescribe
		if err := json.Unmarshal(ok(t, r).Result, &d); err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, p := range d.Params {
			if p.Name == nil {
				parts = append(parts, "?")
			} else {
				parts = append(parts, *p.Name)
			}
		}
		out := "params[" + strings.Join(parts, " ") + "] cols["
		parts = nil
		for _, c := range d.Cols {
			decl := "null"
			if c.Decltype != nil {
				decl = strings.ToUpper(*c.Decltype)
			}
			parts = append(parts, *c.Name+":"+decl)
		}
		out += strings.Join(parts, " ") + "]"
		if d.IsExplain {
			out += " explain"
		}
		if d.IsReadonly {
			out += " readonly"
		}
		return out
	}
	for i, want := range map[int]string{
		2:  "params[:a ?2 @b] cols[x:null ?2:null @b:null id:INTEGER d:DATETIME] readonly",
		6:  "params[?] cols[] ", // RETURNING columns are not described (see README)
		8:  "params[] cols[] readonly",
		9:  "params[? ? ? ? ?5 ? :z] cols[?5:null ?:null :z:null] readonly",
		10: "params[] cols[]",
		12: "params[] cols[1:null] readonly",
	} {
		if got := strings.TrimSpace(describe(res[i])); got != strings.TrimSpace(want) {
			t.Errorf("describe %d: got %q, want %q", i, got, want)
		}
	}
	if got := describe(res[7]); !strings.HasPrefix(got, "params[] cols[addr:null opcode:null") || !strings.HasSuffix(got, "explain readonly") {
		t.Errorf("describe EXPLAIN: %s", got)
	}
	if got := row(execResult(t, res[4]).Rows[0]); got != `integer:"1" integer:"3" integer:"1" text:"x"` {
		t.Errorf("execute stored sql: %s", got)
	}
	if got := failed(t, res[11]); got.Code != "SQLITE_UNKNOWN" {
		t.Errorf("describe error: %+v", got)
	}
	if got := failed(t, res[13]); !strings.Contains(got.Message, "no such table") {
		t.Errorf("sequence error: %+v", got)
	}
	if r := ok(t, res[14]); r.IsAutocommit == nil || !*r.IsAutocommit {
		t.Errorf("get_autocommit: %+v", r)
	}
}

// Protocol errors fail the whole HTTP request, like sqld.
func TestHranaProtocolErrors(t *testing.T) {
	e := newEnv(t, 0)
	for name, body := range map[string]string{
		"invalid json":         `{"baton": `,
		"unknown request":      `{"baton": null, "requests": [{"type": "nope"}]}`,
		"both sql and sql_id":  `{"baton": null, "requests": [{"type": "execute", "stmt": {"sql": "SELECT 1", "sql_id": 1}}]}`,
		"neither sql nor id":   `{"baton": null, "requests": [{"type": "execute", "stmt": {}}]}`,
		"unknown sql_id":       `{"baton": null, "requests": [{"type": "execute", "stmt": {"sql_id": 7}}]}`,
		"duplicate sql_id":     `{"baton": null, "requests": [{"type": "store_sql", "sql_id": 1, "sql": "x"}, {"type": "store_sql", "sql_id": 1, "sql": "y"}]}`,
		"integer as number":    `{"baton": null, "requests": [{"type": "execute", "stmt": {"sql": "SELECT ?", "args": [{"type": "integer", "value": 5}]}}]}`,
		"integer not integral": `{"baton": null, "requests": [{"type": "execute", "stmt": {"sql": "SELECT ?", "args": [{"type": "integer", "value": "1.5"}]}}]}`,
		"float as string":      `{"baton": null, "requests": [{"type": "execute", "stmt": {"sql": "SELECT ?", "args": [{"type": "float", "value": "1.5"}]}}]}`,
		"bad base64":           `{"baton": null, "requests": [{"type": "execute", "stmt": {"sql": "SELECT ?", "args": [{"type": "blob", "base64": "!!"}]}}]}`,
		"unknown value type":   `{"baton": null, "requests": [{"type": "execute", "stmt": {"sql": "SELECT ?", "args": [{"type": "wat"}]}}]}`,
		"request after close":  `{"baton": null, "requests": [{"type": "close"}, {"type": "execute", "stmt": {"sql": "SELECT 1"}}]}`,
		"invalid baton":        `{"baton": "forged", "requests": []}`,
		"and without conds":    `{"baton": null, "requests": [{"type": "batch", "batch": {"steps": [{"stmt": {"sql": "SELECT 1"}}, {"condition": {"type": "and"}, "stmt": {"sql": "SELECT 2"}}]}}]}`,
		"named arg w/o value":  `{"baton": null, "requests": [{"type": "execute", "stmt": {"sql": "SELECT :a", "named_args": [{"name": "a"}]}}]}`,
	} {
		resp, err := http.Post(e.srv.URL+"/v3/pipeline", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		var msg hError
		decodeErr := json.NewDecoder(resp.Body).Decode(&msg)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || decodeErr != nil || msg.Message == "" {
			t.Errorf("%s: got %d %+v (%v)", name, resp.StatusCode, msg, decodeErr)
		}
		if len(resp.Cookies()) != 0 {
			t.Errorf("%s: Hrana must not set cookies", name)
		}
	}
}

func TestHranaCursor(t *testing.T) {
	e := newEnv(t, 0)
	status, head, entries, body := cursor(t, e.srv.URL, nil,
		step(stmt("SELECT 1 AS a UNION ALL SELECT 2"), nil),
		step(stmt("SELECT * FROM nope"), nil),
		step(stmt("SELECT 3"), map[string]any{"type": "ok", "step": 1}),
		step(map[string]any{"sql": "SELECT 4", "want_rows": false}, nil),
		step(stmt("CREATE TABLE t (x)"), nil),
		step(stmt("INSERT INTO t VALUES (?)", 5), nil),
	)
	if status != http.StatusOK {
		t.Fatalf("status %d: %s", status, body)
	}
	if head["baton"] == nil {
		t.Fatalf("an unclosed cursor stream must return a baton: %v", head)
	}
	var got []string
	for _, en := range entries {
		b, _ := json.Marshal(en)
		got = append(got, string(b))
	}
	want := []string{
		`{"cols":[{"decltype":null,"name":"a"}],"step":0,"type":"step_begin"}`,
		`{"row":[{"type":"integer","value":"1"}],"type":"row"}`,
		`{"row":[{"type":"integer","value":"2"}],"type":"row"}`,
		`{"affected_row_count":0,"last_insert_rowid":null,"type":"step_end"}`,
		`{"error":{"code":"SQLITE_UNKNOWN","message":"`, // prefix match below
		`{"cols":[{"decltype":null,"name":"4"}],"step":3,"type":"step_begin"}`,
		`{"affected_row_count":0,"last_insert_rowid":null,"type":"step_end"}`,
		`{"cols":[],"step":4,"type":"step_begin"}`,
		`{"affected_row_count":0,"last_insert_rowid":null,"type":"step_end"}`,
		`{"cols":[],"step":5,"type":"step_begin"}`,
		`{"affected_row_count":1,"last_insert_rowid":1,"type":"step_end"}`, // a number here, as sqld sends
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries:\n%s", len(got), strings.Join(got, "\n"))
	}
	for i := range want {
		if i == 4 {
			if !strings.HasPrefix(got[i], want[i]) || !strings.Contains(got[i], `"step":1,"type":"step_error"`) {
				t.Errorf("entry %d: %s", i, got[i])
			}
			continue
		}
		if got[i] != want[i] {
			t.Errorf("entry %d:\n got %s\nwant %s", i, got[i], want[i])
		}
	}

	// The cursor's stream lives on: its baton works in a pipeline.
	s := e.stream()
	s.baton = ptr(head["baton"].(string))
	if got := s.scalar("SELECT x FROM t").String(); got != `integer:"5"` {
		t.Fatalf("got %s", got)
	}

	// Invalid batches fail before anything is streamed.
	if status, _, _, _ := cursor(t, e.srv.URL, nil, step(stmt("SELECT 1"), map[string]any{"type": "ok", "step": 0})); status != http.StatusBadRequest {
		t.Fatalf("self-referencing condition: got %d", status)
	}
}

// Rows are streamed: a cursor over a result larger than any sane buffer
// arrives complete and in order.
func TestHranaCursorLargeResult(t *testing.T) {
	e := newEnv(t, 0)
	_, _, entries, _ := cursor(t, e.srv.URL, nil, step(stmt(
		"WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 50000) SELECT i, zeroblob(100) FROM c"), nil))
	if len(entries) != 50002 {
		t.Fatalf("got %d entries", len(entries))
	}
	last := entries[50000]["row"].([]any)[0].(map[string]any)["value"]
	if last != "50000" {
		t.Fatalf("last row: %v", last)
	}
}
