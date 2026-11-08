package libsqlhandler

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// database/sql hides the prepared statement, so the SQLite facts Hrana
// reports (parameter names, statement count, statement kind) are recovered
// with a tokenizer that follows SQLite's own (tokenize.c, complete.c).

type tokenKind int

const (
	tokWord  tokenKind = iota // identifier, keyword or number
	tokParam                  // ?, ?NNN, :name, @name, $name, #name
	tokSemi
	tokOpen
	tokClose
	tokOther // strings, quoted identifiers, operators
)

type token struct {
	kind       tokenKind
	text       string
	start, end int
}

func isIDChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '$' || c >= 0x80
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// isSpace includes \v, which SQLite's tokenizer rejects but sqld's parser
// skips: it only matters for telling empty SQL apart, where sqld is the
// reference; inside a real statement SQLite still reports it.
func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'
}

// tokenize splits sql into tokens, dropping whitespace and comments.
// Malformed input (unterminated strings, stray characters) still yields
// tokens; SQLite reports the actual error when the statement runs.
func tokenize(sql string) []token {
	var toks []token
	for i := 0; i < len(sql); {
		c := sql[i]
		start := i
		kind := tokOther
		switch {
		case isSpace(c):
			i++
			continue
		case strings.HasPrefix(sql[i:], "\xef\xbb\xbf"): // UTF-8 BOM, whitespace to SQLite
			i += 3
			continue
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			if end := strings.Index(sql[i+2:], "*/"); end >= 0 {
				i += end + 4
			} else {
				i = len(sql)
			}
			continue
		case c == ';':
			kind, i = tokSemi, i+1
		case c == '(':
			kind, i = tokOpen, i+1
		case c == ')':
			kind, i = tokClose, i+1
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(sql, i, c)
		case c == '[':
			if end := strings.IndexByte(sql[i:], ']'); end >= 0 {
				i += end + 1
			} else {
				i = len(sql)
			}
		case (c == 'x' || c == 'X') && i+1 < len(sql) && sql[i+1] == '\'':
			i = skipQuoted(sql, i+1, '\'')
		case c == '?':
			kind, i = tokParam, i+1
			for i < len(sql) && isDigit(sql[i]) {
				i++
			}
		case c == ':' || c == '@' || c == '$' || c == '#':
			i = scanVariable(sql, i)
			if i > start+1 {
				kind = tokParam
			} else {
				i = start + 1
			}
		case isIDChar(c) || c == '.' && i+1 < len(sql) && isDigit(sql[i+1]):
			kind = tokWord
			number := isDigit(c) || c == '.'
			for i++; i < len(sql) && (isIDChar(sql[i]) || number && sql[i] == '.'); i++ {
			}
		default:
			i++
		}
		toks = append(toks, token{kind: kind, text: sql[start:i], start: start, end: i})
	}
	return toks
}

// skipQuoted returns the index after the quote that closes the one at i;
// a doubled quote is an escaped one.
func skipQuoted(sql string, i int, q byte) int {
	for i++; i < len(sql); i++ {
		if sql[i] == q {
			if i+1 < len(sql) && sql[i+1] == q {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(sql)
}

// scanVariable returns the end of a :name, @name, $name or #name parameter,
// including the TCL-style "::" and "(...)" forms SQLite accepts. It returns
// i+1 when no name follows the prefix.
func scanVariable(sql string, i int) int {
	n := 0
	j := i + 1
	for ; j < len(sql); j++ {
		c := sql[j]
		switch {
		case isIDChar(c):
			n++
		case c == '(' && n > 0:
			for j++; j < len(sql) && !isSpace(sql[j]) && sql[j] != ')'; j++ {
			}
			if j < len(sql) && sql[j] == ')' {
				return j + 1
			}
			return i + 1 // illegal token
		case c == ':' && j+1 < len(sql) && sql[j+1] == ':':
			j++
		default:
			if n == 0 {
				return i + 1
			}
			return j
		}
	}
	if n == 0 {
		return i + 1
	}
	return j
}

// sqlInfo describes the first statement of an SQL text.
type sqlInfo struct {
	count   int        // statements in the text; 0 if only whitespace, comments and semicolons
	text    string     // the first statement, without surrounding comments or semicolon
	body    string     // text without a leading EXPLAIN [QUERY PLAN]
	params  []string   // by SQLite index - 1; "" for nameless
	refs    []paramRef // every parameter token, in text order
	kind    string     // main verb in upper case: SELECT, INSERT, PRAGMA...; for EXPLAIN, the explained one
	explain bool
}

// paramRef locates a parameter token within sqlInfo.text.
type paramRef struct{ index, start, end int }

// sqlite3_complete's state machine, which knows that the semicolons inside
// CREATE TRIGGER ... BEGIN ... END do not end the statement.
const (
	ckSemi = iota
	ckOther
	ckExplain
	ckCreate
	ckTemp
	ckTrigger
	ckEnd
)

var completeTrans = [8][7]int{
	/*            SEMI OTHER EXPLAIN CREATE TEMP TRIGGER END */
	/* 0 INVALID */ {1, 2, 3, 4, 2, 2, 2},
	/* 1 START   */ {1, 2, 3, 4, 2, 2, 2},
	/* 2 NORMAL  */ {1, 2, 2, 2, 2, 2, 2},
	/* 3 EXPLAIN */ {1, 3, 2, 4, 2, 2, 2},
	/* 4 CREATE  */ {1, 2, 2, 2, 4, 5, 2},
	/* 5 TRIGGER */ {6, 5, 5, 5, 5, 5, 5},
	/* 6 SEMI    */ {6, 5, 5, 5, 5, 5, 7},
	/* 7 END     */ {1, 5, 5, 5, 5, 5, 5},
}

func completeClass(t token) int {
	switch t.kind {
	case tokSemi:
		return ckSemi
	case tokWord:
		switch strings.ToUpper(t.text) {
		case "EXPLAIN":
			return ckExplain
		case "CREATE":
			return ckCreate
		case "TEMP", "TEMPORARY":
			return ckTemp
		case "TRIGGER":
			return ckTrigger
		case "END":
			return ckEnd
		}
	}
	return ckOther
}

// maxVariableNumber is SQLite's default SQLITE_MAX_VARIABLE_NUMBER.
const maxVariableNumber = 32766

// analyzeSQL splits sql into statements and describes the first one.
func analyzeSQL(sql string) (sqlInfo, error) {
	// SQLite stops reading at a NUL byte.
	if i := strings.IndexByte(sql, 0); i >= 0 {
		sql = sql[:i]
	}
	var info sqlInfo
	var first []token
	state, inStmt := 0, false
	for _, t := range tokenize(sql) {
		state = completeTrans[state][completeClass(t)]
		if t.kind == tokSemi && state == 1 {
			inStmt = false
			continue
		}
		if !inStmt {
			inStmt = true
			info.count++
		}
		if info.count == 1 {
			first = append(first, t)
		}
	}
	if info.count == 0 {
		return info, nil
	}
	info.text = sql[first[0].start:first[len(first)-1].end]

	// Parameter numbering as in sqlite3ExprAssignVarNumber.
	byName := map[string]int{}
	for _, t := range first {
		if t.kind != tokParam {
			continue
		}
		index := 0
		switch {
		case t.text == "?":
			info.params = append(info.params, "")
			index = len(info.params)
		case t.text[0] == '?':
			n, err := strconv.Atoi(t.text[1:])
			if err != nil || n < 1 || n > maxVariableNumber {
				return info, fmt.Errorf("variable number must be between ?1 and ?%d", maxVariableNumber)
			}
			for len(info.params) < n {
				info.params = append(info.params, "")
			}
			if _, named := byName[info.params[n-1]]; !named {
				info.params[n-1] = t.text
				byName[t.text] = n
			}
			index = n
		default:
			if _, seen := byName[t.text]; !seen {
				info.params = append(info.params, t.text)
				byName[t.text] = len(info.params)
			}
			index = byName[t.text]
		}
		base := first[0].start
		info.refs = append(info.refs, paramRef{index, t.start - base, t.end - base})
	}

	words := first
	if len(words) > 0 && strings.EqualFold(words[0].text, "EXPLAIN") {
		info.explain = true
		words = words[1:]
		if len(words) > 1 && strings.EqualFold(words[0].text, "QUERY") && strings.EqualFold(words[1].text, "PLAN") {
			words = words[2:]
		}
	}
	info.kind = statementKind(words)
	info.body = info.text
	if len(words) > 0 {
		info.body = sql[words[0].start:first[len(first)-1].end]
	}
	return info, nil
}

// statementKind returns the verb of a statement, looking past a WITH clause.
func statementKind(toks []token) string {
	if len(toks) == 0 || toks[0].kind != tokWord {
		return ""
	}
	kind := strings.ToUpper(toks[0].text)
	if kind != "WITH" {
		return kind
	}
	// The verb follows the closing parenthesis of the last CTE body; a CTE
	// name (even one called "replace") never does.
	depth := 0
	for i, t := range toks[1:] {
		switch t.kind {
		case tokOpen:
			depth++
		case tokClose:
			depth--
		case tokWord:
			if depth == 0 && toks[i].kind == tokClose {
				switch w := strings.ToUpper(t.text); w {
				case "SELECT", "VALUES", "INSERT", "REPLACE", "UPDATE", "DELETE":
					return w
				}
			}
		}
	}
	return kind
}

// driverArgs returns the statement (or, with body, the statement without
// EXPLAIN) and its database/sql arguments, given one value per parameter.
// Named parameters bind by bare name, except where database/sql or the
// driver cannot: a bare name not starting with a letter (:1x), or one shared
// by two prefixes (:a and @a). Those are rewritten to ?N, which binds by
// position, as are numbers with leading zeros. restore maps result column names, which SQLite takes from the
// statement text, back to the original parameter names.
func (info *sqlInfo) driverArgs(vals []any, body bool) (text string, args []any, restore func(string) string) {
	args = make([]any, len(info.params))
	positional := map[int]bool{}
	owner := map[string]string{} // bare name -> the parameter binding it
	for i, name := range info.params {
		if name == "" || name[0] == '?' {
			args[i] = vals[i]
			continue
		}
		bare := name[1:]
		r, _ := utf8.DecodeRuneInString(bare)
		if prev, taken := owner[bare]; (taken && prev != name) || !unicode.IsLetter(r) {
			positional[i+1] = true
			args[i] = vals[i]
			continue
		}
		owner[bare] = name
		args[i] = sql.Named(bare, vals[i])
	}

	text, offset := info.text, 0
	if body {
		text, offset = info.body, len(info.text)-len(info.body)
	}
	var b strings.Builder
	rewritten := map[int]bool{}
	last := 0
	for _, ref := range info.refs {
		tok := info.text[ref.start:ref.end]
		canonical := "?" + strconv.Itoa(ref.index)
		// modernc reads ?001 as a parameter named "001": write it as ?1.
		if ref.start < offset || !positional[ref.index] && (tok[0] != '?' || tok == "?" || tok == canonical) {
			continue
		}
		rewritten[ref.index] = true
		b.WriteString(text[last : ref.start-offset])
		b.WriteString(canonical)
		last = ref.end - offset
	}
	restore = func(name string) string { return name }
	if len(rewritten) == 0 {
		return text, args, restore
	}
	b.WriteString(text[last:])
	restore = func(name string) string {
		return numberedParam.ReplaceAllStringFunc(name, func(m string) string {
			if n, _ := strconv.Atoi(m[1:]); rewritten[n] {
				return info.params[n-1]
			}
			return m
		})
	}
	return b.String(), args, restore
}

var numberedParam = regexp.MustCompile(`\?[0-9]+`)
