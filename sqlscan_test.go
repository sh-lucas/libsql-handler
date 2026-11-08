package libsqlhandler

import (
	"database/sql"
	"strings"
	"testing"
)

func TestAnalyzeSQL(t *testing.T) {
	for _, c := range []struct {
		sql    string
		count  int
		text   string
		params string // names joined by space, "?" for nameless
		kind   string
	}{
		{"", 0, "", "", ""},
		{" ;; -- c\n /* x */ ;", 0, "", "", ""},
		{"SELECT 1", 1, "SELECT 1", "", "SELECT"},
		{"  select 1 ; -- trailing", 1, "select 1", "", "SELECT"},
		{";SELECT 1;;", 1, "SELECT 1", "", "SELECT"},
		{"SELECT 1; SELECT 2", 2, "SELECT 1", "", "SELECT"},
		{"SELECT ';', \";\", `;`, [;] -- ;\n", 1, "SELECT ';', \";\", `;`, [;]", "", "SELECT"},
		{"SELECT 'it''s; fine'", 1, "SELECT 'it''s; fine'", "", "SELECT"},
		{"SELECT 'unterminated; SELECT 2", 1, "SELECT 'unterminated; SELECT 2", "", "SELECT"},
		{"CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; SELECT CASE WHEN 1 THEN 2 END; END; SELECT 3", 2,
			"CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; SELECT CASE WHEN 1 THEN 2 END; END", "", "CREATE"},
		{"create temp trigger tr after insert on t begin select :a; end", 1,
			"create temp trigger tr after insert on t begin select :a; end", ":a", "CREATE"},

		{"SELECT ?, ?, ?", 1, "", "? ? ?", "SELECT"},
		{"SELECT ?2, ?1, ?", 1, "", "?1 ?2 ?", "SELECT"},
		{"SELECT ?5, ?, :z", 1, "", "? ? ? ? ?5 ? :z", "SELECT"},
		{"SELECT :a, @a, $a, #a, :a, ?", 1, "", ":a @a $a #a ?", "SELECT"},
		{"SELECT ?, ?1", 1, "", "?1", "SELECT"},
		{"SELECT :a, ?1", 1, "", ":a", "SELECT"},
		{"SELECT $a::b::c, $f(x y), $g(z), :é", 1, "", "$a::b::c $g(z) :é", "SELECT"},
		{"SELECT x':a', ':b', \":c\", [:d], `:e`, a$b -- :f\n /* :g */", 1, "", "", "SELECT"},
		{"SELECT 1.5, .5, 1e5, t.x, ? ", 1, "", "?", "SELECT"},
		{"SELECT : a, @ , $", 1, "", "", "SELECT"},

		{"INSERT INTO t VALUES (?)", 1, "", "?", "INSERT"},
		{"WITH q AS (SELECT 1), r(x) AS MATERIALIZED (SELECT 2) INSERT INTO t SELECT * FROM q", 1, "", "", "INSERT"},
		{"with recursive c(i) as (select 1 union all select i+1 from c) select * from c", 1, "", "", "SELECT"},
		{"EXPLAIN QUERY PLAN DELETE FROM t", 1, "", "", "DELETE"},
		{"(SELECT 1)", 1, "", "", ""},
		{"WITH replace AS (SELECT 1), update(x) AS (SELECT 2) SELECT * FROM replace", 1, "", "", "SELECT"},
		{"WITH c AS MATERIALIZED (SELECT 1) DELETE FROM t", 1, "", "", "DELETE"},
		{"\xef\xbb\xbfINSERT INTO t VALUES (?)", 1, "INSERT INTO t VALUES (?)", "?", "INSERT"},
		{"\xef\xbb\xbfCREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END", 1, "", "", "CREATE"},
		{"SELECT :a\x00, :b; SELECT 2", 1, "SELECT :a", ":a", "SELECT"},
	} {
		info, err := analyzeSQL(c.sql)
		if err != nil {
			t.Errorf("%q: %v", c.sql, err)
			continue
		}
		var params []string
		for _, p := range info.params {
			if p == "" {
				p = "?"
			}
			params = append(params, p)
		}
		if info.count != c.count || info.kind != c.kind || strings.Join(params, " ") != c.params ||
			(c.text != "" && info.text != c.text) {
			t.Errorf("%q:\n got count=%d kind=%q params=%q text=%q\nwant count=%d kind=%q params=%q text=%q",
				c.sql, info.count, info.kind, strings.Join(params, " "), info.text, c.count, c.kind, c.params, c.text)
		}
	}

	for _, sql := range []string{"SELECT ?0", "SELECT ?32767", "SELECT ?99999999999999999999"} {
		if _, err := analyzeSQL(sql); err == nil {
			t.Errorf("%q: want error", sql)
		}
	}
	if info, _ := analyzeSQL("EXPLAIN SELECT 1"); !info.explain {
		t.Errorf("EXPLAIN not detected")
	}
}

func FuzzTokenizeTerminates(f *testing.F) {
	f.Add("SELECT ?1, :a, $b(c), 'x''y', /* z")
	f.Fuzz(func(t *testing.T, sql string) {
		info, err := analyzeSQL(sql)
		if err == nil && info.count > 0 && !strings.Contains(sql, info.text) {
			t.Fatalf("text %q not in %q", info.text, sql)
		}
	})
}

func TestDriverArgs(t *testing.T) {
	for _, c := range []struct{ sql, text, named string }{
		{"SELECT :a, ?, @b", "SELECT :a, ?, @b", "a b"},
		// database/sql rejects names not starting with a letter.
		{"SELECT :1x, :a, :1x", "SELECT ?1, :a, ?1", "a"},
		// :a and @a would share the bare name a; the later one goes positional.
		{"SELECT :a, @a, $a, :a", "SELECT :a, ?2, ?3, :a", "a"},
		{"EXPLAIN SELECT @a, :a", "EXPLAIN SELECT @a, ?2", "a"},
		{"SELECT ?001, ?2, ?", "SELECT ?1, ?2, ?", ""},
	} {
		info, err := analyzeSQL(c.sql)
		if err != nil {
			t.Fatal(err)
		}
		text, args, restore := info.driverArgs(make([]any, len(info.params)), false)
		var named []string
		for _, a := range args {
			if n, ok := a.(sql.NamedArg); ok {
				named = append(named, n.Name)
			}
		}
		if text != c.text || strings.Join(named, " ") != c.named {
			t.Errorf("%q: got %q named %v, want %q named %q", c.sql, text, named, c.text, c.named)
		}
		if got := restore(text); got != c.sql {
			t.Errorf("%q: restore gave %q", c.sql, got)
		}
	}
	info, _ := analyzeSQL("EXPLAIN QUERY PLAN SELECT @a, :a")
	if body, _, _ := info.driverArgs(make([]any, 2), true); body != "SELECT @a, ?2" {
		t.Errorf("body: %q", body)
	}
}
