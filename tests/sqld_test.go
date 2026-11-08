package tests

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// Differential tests: the same requests go to the handler and to a real sqld
// (SQLD_URL, e.g. `just sqld` then `just test-sqld`), and the responses must
// match once the fields that legitimately differ are normalized away.
// Skipped when SQLD_URL is unset.

func sqldURL(t testing.TB) string {
	t.Helper()
	u := os.Getenv("SQLD_URL")
	if u == "" {
		t.Skip("SQLD_URL not set")
	}
	return u
}

func TestMatchesRealSqld(t *testing.T) {
	url := sqldURL(t)
	e := newEnv(t, 0)

	for _, body := range []string{
		`{"statements": ["SELECT 42 AS num, 'hello' AS msg, NULL AS empty, 1.5 AS f"]}`,
		`{"statements": ["SELECT 1", "SELECT * FROM missing_table_123", "SELECT 2"]}`,
		`{"statements": [{"q": "SELECT ?, :n", "params": [1]}]}`,
		`{"statements": [{"q": "SELECT :a, @b", "params": {":a": 1, "b": "x"}}]}`,
	} {
		// rows_read is SQLite's scan count in sqld, and error texts come from
		// different SQLite bindings: both are normalized away.
		got, want := normalized(t, e.srv.URL, body), normalized(t, url, body)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s\n handler: %v\n    sqld: %v", body, got, want)
		}
	}
}

func normalized(t *testing.T, url, body string) any {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	stripTimings(v)
	return map[string]any{"status": resp.StatusCode, "body": v}
}

func stripTimings(v any) {
	switch x := v.(type) {
	case map[string]any:
		delete(x, "query_duration_ms")
		delete(x, "rows_read")
		if _, ok := x["error"].(string); ok {
			x["error"] = "<error>"
		}
		for _, c := range x {
			stripTimings(c)
		}
	case []any:
		for _, c := range x {
			stripTimings(c)
		}
	}
}

// hranaCases are pipelines replayed on one stream per case. Tables are
// dropped first because sqld's database outlives the test run.
var hranaCases = map[string][][]map[string]any{
	"values": {{
		execSQL("SELECT ?, ?, ?, ?, ?, ?, ?", nil, int64(math.MaxInt64), int64(math.MinInt64), 1.5, "héllo\x00", []byte{0, 0xff, 1}, math.Copysign(0, -1)),
		execSQL("SELECT 1e999, -1e999, 9223372036854775807 + 1, 0.1 + 0.2, 1e300 * 10, x'', ''"),
		execSQL("SELECT typeof(?), typeof(?), typeof(?), typeof(?), typeof(?)", nil, 1, 1.0, "1", []byte("1")),
		closeReq,
	}},
	"results and counters": {{
		map[string]any{"type": "sequence", "sql": "DROP TABLE IF EXISTS ab_a; DROP TABLE IF EXISTS ab_log; CREATE TABLE ab_a (id INTEGER PRIMARY KEY, v INT UNIQUE, d DATETIME, b BLOB, t VARCHAR(10)); CREATE TABLE ab_log (x); CREATE TRIGGER ab_tr AFTER INSERT ON ab_a BEGIN INSERT INTO ab_log VALUES (new.v); END;"},
		execSQL("INSERT INTO ab_a (v, b, t) VALUES (10, x'00', 'x'), (20, NULL, 'y')"),
		execSQL("SELECT 1"),
		execSQL("UPDATE ab_a SET v = v + 1"),
		execSQL("UPDATE ab_a SET v = v + 1 WHERE 0"),
		execSQL("INSERT INTO ab_a (v) SELECT 5 WHERE 0"),
		execSQL("DELETE FROM ab_a WHERE id = 1"),
		execSQL("INSERT INTO ab_a (v) VALUES (21)"),
		execSQL("REPLACE INTO ab_a (id, v) VALUES (7, 99)"),
		execSQL("INSERT OR IGNORE INTO ab_a (v) VALUES (99)"),
		execSQL("WITH q AS (SELECT 1) INSERT INTO ab_log SELECT * FROM q"),
		execSQL("INSERT INTO ab_a (v) VALUES (1), (2) RETURNING id, v, t"),
		execSQL("SELECT id, v AS vv, b, t, v + 1, (SELECT max(x) FROM ab_log) FROM ab_a ORDER BY id"),
		// want_rows=false is left out: sqld ignores it, the handler follows the spec.
		execSQL("UPDATE ab_a SET v = -v RETURNING v"),
		execSQL("SELECT count(*), sum(v) FROM ab_a"),
		map[string]any{"type": "sequence", "sql": "DROP TABLE ab_a; DROP TABLE ab_log"},
		closeReq,
	}},
	"parameters": {{
		execSQL("SELECT ?, ?", 1, "x"),
		execSQL("SELECT ?2, ?1, ?", 1, 2, 3),
		execSQL("SELECT ?3", nil, nil, 3),
		execSQL("SELECT ?002, ?1", 1, 2),
		execSQL("SELECT ?, ?", 1),
		execSQL("SELECT ?", 1, 2),
		execSQL("SELECT 1", 1),
		execSQL("SELECT :a", 1),
		execSQL("SELECT ?1, :a", 1, 2),
		execute(map[string]any{"sql": "SELECT :a, @b, $c", "named_args": []any{
			map[string]any{"name": ":a", "value": val(1)}, map[string]any{"name": "@b", "value": val(2)}, map[string]any{"name": "$c", "value": val(3)}}}),
		execute(map[string]any{"sql": "SELECT :a, @b, $c, :a", "named_args": []any{
			map[string]any{"name": "a", "value": val(1)}, map[string]any{"name": "b", "value": val(2)}, map[string]any{"name": "c", "value": val(3)}}}),
		execute(map[string]any{"sql": "SELECT :a", "named_args": []any{
			map[string]any{"name": "b", "value": val(1)}}}),
		execute(map[string]any{"sql": "SELECT :a, @a, $a, :1x, :a + 1, :1x", "named_args": []any{
			map[string]any{"name": ":a", "value": val(1)}, map[string]any{"name": "@a", "value": val(2)},
			map[string]any{"name": "$a", "value": val(3)}, map[string]any{"name": ":1x", "value": val(4)}}}),
		map[string]any{"type": "describe", "sql": "SELECT :1x, @a, :a"},
		execute(map[string]any{"sql": "SELECT :a", "named_args": []any{
			map[string]any{"name": "a", "value": val(1)}, map[string]any{"name": "zz", "value": val(2)}}}),
		execute(map[string]any{"sql": "SELECT ?", "named_args": []any{
			map[string]any{"name": "a", "value": val(1)}}}),
		execute(map[string]any{"sql": "SELECT :a, ?", "args": vals(1), "named_args": []any{
			map[string]any{"name": "a", "value": val(1)}}}),
		closeReq,
	}},
	"statement errors": {{
		map[string]any{"type": "sequence", "sql": "DROP TABLE IF EXISTS ab_u; CREATE TABLE ab_u (x UNIQUE NOT NULL CHECK (x > 0))"},
		execSQL("INSERT INTO ab_u VALUES (1), (1)"),
		execSQL("INSERT INTO ab_u VALUES (NULL)"),
		execSQL("INSERT INTO ab_u VALUES (-1)"),
		execSQL("SELECT * FROM missing_table"),
		execSQL(""),
		execSQL(";"),
		execSQL("  -- only a comment"),
		execSQL("SELECT 1; SELECT 2"),
		execSQL("SELECT 1;"),
		execSQL("SELECT 1;;"),
		execSQL(";SELECT 1"),
		execSQL("SELECT 1; -- trailing"),
		execSQL("SELECT ';'; "),
		execSQL("SELECT \";\""),
		execSQL("COMMIT"),
		execSQL("SELECT count(*) FROM ab_u"),
		map[string]any{"type": "sequence", "sql": "DROP TABLE ab_u"},
		closeReq,
	}},
	"batch": {{
		batchReq(
			step(stmt("BEGIN"), nil),
			step(stmt("SELECT * FROM missing"), map[string]any{"type": "ok", "step": 0}),
			step(stmt("SELECT 'skipped'"), map[string]any{"type": "ok", "step": 1}),
			step(stmt("SELECT 'ran'"), map[string]any{"type": "error", "step": 1}),
			step(stmt("SELECT 'in tx'"), map[string]any{"type": "not", "cond": map[string]any{"type": "is_autocommit"}}),
			step(stmt("SELECT 'and'"), map[string]any{"type": "and", "conds": []any{}}),
			step(stmt("SELECT 'or'"), map[string]any{"type": "or", "conds": []any{}}),
			step(stmt("SELECT 'or2'"), map[string]any{"type": "or", "conds": []any{map[string]any{"type": "ok", "step": 2}, map[string]any{"type": "ok", "step": 3}}}),
			step(stmt("SELECT 'skipped error'"), map[string]any{"type": "error", "step": 2}),
			step(stmt("ROLLBACK"), nil),
			step(stmt("SELECT 'autocommit'"), map[string]any{"type": "is_autocommit"}),
		),
		map[string]any{"type": "get_autocommit"},
		closeReq,
	}},
	"interactive transaction": {
		{map[string]any{"type": "sequence", "sql": "DROP TABLE IF EXISTS ab_t; CREATE TABLE ab_t (x)"}, execSQL("BEGIN"), execSQL("INSERT INTO ab_t VALUES (1)")},
		{map[string]any{"type": "get_autocommit"}, execSQL("SELECT count(*) FROM ab_t")},
		{execSQL("ROLLBACK"), map[string]any{"type": "get_autocommit"}, execSQL("SELECT count(*) FROM ab_t")},
		{map[string]any{"type": "sequence", "sql": "DROP TABLE ab_t"}, closeReq},
	},
	"sequence, store_sql, describe": {{
		map[string]any{"type": "sequence", "sql": "DROP TABLE IF EXISTS ab_dd; CREATE TABLE ab_dd (id INTEGER PRIMARY KEY, d DateTime); INSERT INTO ab_dd (d) VALUES ('x'); SELECT 5"},
		map[string]any{"type": "store_sql", "sql_id": 1, "sql": "SELECT :a AS x, ?2, @b, id, d FROM ab_dd"},
		map[string]any{"type": "describe", "sql_id": 1},
		map[string]any{"type": "store_sql", "sql_id": 2, "sql": "SELECT :a, @b, id, d FROM ab_dd"},
		execute(map[string]any{"sql_id": 2, "named_args": []any{
			map[string]any{"name": "a", "value": val(1)}, map[string]any{"name": "@b", "value": val(3)}}}),
		map[string]any{"type": "close_sql", "sql_id": 99},
		map[string]any{"type": "describe", "sql": "EXPLAIN SELECT 1"},
		map[string]any{"type": "describe", "sql": "BEGIN"},
		map[string]any{"type": "describe", "sql": "SELECT ?5, ?, :z"},
		map[string]any{"type": "describe", "sql": "SELECT :a, :a, @a, $a"},
		map[string]any{"type": "describe", "sql": "CREATE TABLE ab_zz (a)"},
		map[string]any{"type": "describe", "sql": "DELETE FROM ab_dd"},
		map[string]any{"type": "describe", "sql": "EXPLAIN DELETE FROM ab_dd"},
		map[string]any{"type": "describe", "sql": "EXPLAIN QUERY PLAN SELECT * FROM ab_dd WHERE id = ?"},
		map[string]any{"type": "describe", "sql": "PRAGMA foreign_keys = ON"},
		map[string]any{"type": "describe", "sql": "SELECT a, a, b FROM (SELECT 1 AS a, 2 AS b)"},
		map[string]any{"type": "describe", "sql": "SELECT * FROM nope"},
		map[string]any{"type": "describe", "sql": "WITH q AS (SELECT 1) SELECT * FROM q"},
		map[string]any{"type": "describe", "sql": "VALUES (1, 'a')"},
		map[string]any{"type": "describe", "sql": "SELECT 1; SELECT 2"},
		map[string]any{"type": "describe", "sql": " ; -- nothing"},
		map[string]any{"type": "sequence", "sql": "SELECT * FROM nope"},
		map[string]any{"type": "sequence", "sql": ""},
		map[string]any{"type": "sequence", "sql": "DROP TABLE ab_dd"},
		closeReq,
	}},
	"protocol errors": {
		{execute(map[string]any{"sql": "SELECT 1", "sql_id": 1})},
		{execute(map[string]any{})},
		{execute(map[string]any{"sql_id": 7})},
		{map[string]any{"type": "store_sql", "sql_id": 1, "sql": "x"}, map[string]any{"type": "store_sql", "sql_id": 1, "sql": "y"}},
		{closeReq, execSQL("SELECT 1")},
		{map[string]any{"type": "nope"}},
		{batchReq(step(stmt("SELECT 1"), map[string]any{"type": "ok", "step": 0}))},
		{batchReq(step(stmt("SELECT 1"), nil), step(stmt("SELECT 1"), map[string]any{"type": "error", "step": 3}))},
	},
}

// hranaNormalize drops what may differ: batons (random), timings and stats
// (sqld counts scanned rows), replication indexes, and error messages (the
// driver words them differently; codes are compared). decltype is
// uppercased because database/sql drivers report it that way.
func hranaNormalize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for _, k := range []string{"query_duration_ms", "rows_read", "rows_written", "replication_index"} {
			delete(x, k)
		}
		if b, ok := x["baton"]; ok && b != nil {
			x["baton"] = "<baton>"
		}
		if d, ok := x["decltype"].(string); ok {
			x["decltype"] = strings.ToUpper(d)
		}
		if _, ok := x["message"].(string); ok {
			x["message"] = "<message>"
		}
		// sqld parses SQL itself before SQLite does and has its own codes for that.
		if c, ok := x["code"].(string); ok && (c == "SQL_PARSE_ERROR" || c == "SQL_INPUT_ERROR") {
			x["code"] = "SQLITE_UNKNOWN"
		}
		for k, c := range x {
			x[k] = hranaNormalize(c)
		}
	case []any:
		for i, c := range x {
			x[i] = hranaNormalize(c)
		}
	}
	return v
}

// replay runs the pipelines of one case on a single stream and returns the
// normalized responses. A failed HTTP request ends the case.
func replay(t testing.TB, url string, pipelines [][]map[string]any) []any {
	t.Helper()
	var baton any
	var out []any
	for _, reqs := range pipelines {
		b, _ := json.Marshal(map[string]any{"baton": baton, "requests": reqs})
		resp, err := http.Post(url+"/v3/pipeline", "application/json", strings.NewReader(string(b)))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			out = append(out, fmt.Sprintf("HTTP %d", resp.StatusCode))
			return out
		}
		var v map[string]any
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		baton = v["baton"]
		out = append(out, hranaNormalize(v))
	}
	if baton != nil {
		b, _ := json.Marshal(map[string]any{"baton": baton, "requests": []any{closeReq}})
		resp, err := http.Post(url+"/v3/pipeline", "application/json", strings.NewReader(string(b)))
		if err == nil {
			resp.Body.Close()
		}
	}
	return out
}

func diffJSON(t testing.TB, name string, got, want any) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		return
	}
	g, _ := json.MarshalIndent(got, "", " ")
	w, _ := json.MarshalIndent(want, "", " ")
	gl, wl := strings.Split(string(g), "\n"), strings.Split(string(w), "\n")
	for i := 0; i < max(len(gl), len(wl)); i++ {
		var a, b string
		if i < len(gl) {
			a = gl[i]
		}
		if i < len(wl) {
			b = wl[i]
		}
		if a != b {
			lo := max(i-8, 0)
			t.Errorf("%s: first difference at line %d\n handler: %s\n    sqld: %s\ncontext (handler):\n%s",
				name, i, a, b, strings.Join(gl[lo:min(i+3, len(gl))], "\n"))
			return
		}
	}
}

func TestHranaMatchesRealSqld(t *testing.T) {
	url := sqldURL(t)
	e := newEnv(t, 0)
	for name, pipelines := range hranaCases {
		t.Run(name, func(t *testing.T) {
			diffJSON(t, name, replay(t, e.srv.URL, pipelines), replay(t, url, pipelines))
		})
	}
}

func TestHranaCursorMatchesRealSqld(t *testing.T) {
	url := sqldURL(t)
	e := newEnv(t, 0)
	steps := []map[string]any{
		step(stmt("DROP TABLE IF EXISTS ab_c"), nil),
		step(stmt("CREATE TABLE ab_c (x INT)"), nil),
		step(stmt("INSERT INTO ab_c VALUES (1), (2) RETURNING x"), nil),
		step(stmt("SELECT * FROM nope"), nil),
		step(stmt("SELECT 'skipped'"), map[string]any{"type": "ok", "step": 3}),
		step(stmt("SELECT x AS y, x FROM ab_c"), nil),
		step(stmt("SELECT ?, ?", 1), nil),
		step(stmt("SELECT x, ? FROM ab_c", []byte("b")), nil),
		step(stmt("DROP TABLE ab_c"), nil),
	}
	lines := func(url string) []any {
		status, head, entries, body := cursor(t, url, nil, steps...)
		if status != http.StatusOK {
			t.Fatalf("%s: %d %s", url, status, body)
		}
		var out []any
		out = append(out, hranaNormalize(head))
		for i, en := range entries {
			if en["type"] == "replication_index" { // sqld extension
				continue
			}
			// A step_error may come with or without a step_begin before it
			// (sqld binds arguments after reporting columns); both are valid.
			if en["type"] == "step_begin" && i+1 < len(entries) && entries[i+1]["type"] == "step_error" {
				continue
			}
			out = append(out, hranaNormalize(en))
		}
		return out
	}
	diffJSON(t, "cursor", lines(e.srv.URL), lines(url))
}

// FuzzHranaValuesMatchSqld checks value encoding both ways against sqld:
// `just fuzz-sqld`. No text/number conversions: SQLite versions differ there
// (sqld's prints floats with fewer digits, casts '-' to -0.0).
func FuzzHranaValuesMatchSqld(f *testing.F) {
	f.Add(int64(0), 0.0, "", []byte{})
	f.Add(int64(math.MaxInt64), math.SmallestNonzeroFloat64, "é\x00\U0001F600", []byte{0, 0xff})
	f.Add(int64(math.MinInt64), -math.MaxFloat64, "'; DROP TABLE x; --", []byte("\xff\xfe"))
	f.Add(int64(1)<<53+1, 1e21, "�\x7f", []byte{1, 2, 3, 4, 5})
	f.Add(int64(-1), 0.1, "2024-01-01", []byte("text"))
	url := sqldURL(f)
	e := newEnv(f, 0)
	f.Fuzz(func(t *testing.T, i int64, fl float64, s string, b []byte) {
		if math.IsInf(fl, 0) || math.IsNaN(fl) {
			t.Skip("not representable in JSON")
		}
		reqs := [][]map[string]any{{
			execSQL("SELECT ?, ?, ?, ?, typeof(?3), length(?4), ?1 + 1, hex(?4), quote(?3)", i, fl, s, b),
			closeReq,
		}}
		diffJSON(t, "values", roundFloats(replay(t, e.srv.URL, reqs)), roundFloats(replay(t, url, reqs)))
	})
}

// roundFloats rounds float values to 14 digits: sqld's JSON parser is off by
// an ulp for some inputs (it reads -8.988465674311579e+307 back as
// -8.98846567431158e+307), while the handler round-trips them exactly.
func roundFloats(v any) any {
	switch x := v.(type) {
	case map[string]any:
		if f, ok := x["value"].(float64); ok && x["type"] == "float" {
			x["value"] = strconv.FormatFloat(f, 'g', 14, 64)
		}
		for _, c := range x {
			roundFloats(c)
		}
	case []any:
		for _, c := range x {
			roundFloats(c)
		}
	}
	return v
}

// FuzzHranaDescribeMatchesSqld compares parameter discovery and statement
// splitting, which the handler does with its own tokenizer, against SQLite
// as embedded in sqld.
func FuzzHranaDescribeMatchesSqld(f *testing.F) {
	for _, sql := range []string{
		"SELECT ?, ?3, :a, @b, $c, :a, ?",
		"SELECT ':x', \":y\", [?], `@z`, x'00', ?1 -- :q\n, ? /* $w */",
		"SELECT $a::b, $c(d), :e",
		"CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT :x; SELECT ?; END",
		"SELECT 1; SELECT 2",
		"SELECT 1;",
		";",
		"WITH q AS (SELECT ?) SELECT * FROM q",
		"EXPLAIN QUERY PLAN SELECT ?",
		"SELECT ?999",
		"PRAGMA journal_mode = WAL",
		"WITH replace AS (SELECT 1) SELECT * FROM replace",
	} {
		f.Add(sql)
	}
	url := sqldURL(f)
	e := newEnv(f, 0)
	f.Fuzz(func(t *testing.T, sql string) {
		reqs := [][]map[string]any{{
			map[string]any{"type": "describe", "sql": sql},
			execSQL(sql),
			closeReq,
		}}
		got, want := replay(t, e.srv.URL, reqs), replay(t, url, reqs)
		// Only compare what both sides agree is valid SQLite: sqld's own
		// parser rejects some statements SQLite accepts, and syntax error
		// codes differ.
		if len(got) != 1 || len(want) != 1 {
			diffJSON(t, "http status", got, want)
			return
		}
		g := got[0].(map[string]any)["results"].([]any)
		w := want[0].(map[string]any)["results"].([]any)
		gd, wd := g[0].(map[string]any), w[0].(map[string]any)
		if gd["type"] == "error" && wd["type"] == "ok" {
			t.Fatalf("%q: handler rejects what sqld describes: %v", sql, gd["error"])
		}
		if gd["type"] == "ok" && wd["type"] == "ok" {
			gr := gd["response"].(map[string]any)["result"].(map[string]any)
			wr := wd["response"].(map[string]any)["result"].(map[string]any)
			// Columns are left out: see the README for what describe reports.
			for _, k := range []string{"params", "is_explain", "is_readonly"} {
				diffJSON(t, "describe "+k, gr[k], wr[k])
			}
		}
		ge, we := g[1].(map[string]any), w[1].(map[string]any)
		gc, _ := ge["error"].(map[string]any)
		wc, _ := we["error"].(map[string]any)
		// Which error wins when the SQL is also invalid differs; skip those.
		isCount := func(e map[string]any) bool {
			return e == nil || e["code"] == "SQL_NO_STATEMENT" || e["code"] == "SQL_MANY_STATEMENTS"
		}
		if !isCount(gc) || !isCount(wc) {
			return
		}
		// sqld runs "SELECT 1; garbage" as SELECT 1, silently dropping a tail
		// it cannot parse; the handler rejects any second statement.
		if gc != nil && gc["code"] == "SQL_MANY_STATEMENTS" && wc == nil {
			return
		}
		for _, code := range []string{"SQL_NO_STATEMENT", "SQL_MANY_STATEMENTS"} {
			if (gc != nil && gc["code"] == code) != (wc != nil && wc["code"] == code) {
				t.Errorf("%q: statement count disagrees: handler %v, sqld %v", sql, gc, wc)
			}
		}
	})
}
