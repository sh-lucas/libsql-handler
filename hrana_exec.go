package libsqlhandler

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Statement execution shared by the pipeline and cursor endpoints. Each
// function returns an in-band *hranaError (the statement failed, the stream
// goes on) or a fatal error (protocolError, or the client went away).

// resolveSQL returns the text of a statement given inline or by sql_id.
func (s *session) resolveSQL(text *string, id *int32) (string, error) {
	switch {
	case text != nil && id != nil:
		return "", errProtocol("Either `sql` or `sql_id` are required, but not both")
	case text != nil:
		return *text, nil
	case id != nil:
		if stored, ok := s.sqls[*id]; ok {
			return stored, nil
		}
		return "", errProtocol(fmt.Sprintf("SQL text %d not found", *id))
	}
	return "", errProtocol("Either `sql` or `sql_id` are required")
}

// prepareStmt analyzes one statement and binds its arguments; info.text is
// the statement as the driver should run it.
func (s *session) prepareStmt(st *hranaStmt) (sqlInfo, []any, func(string) string, *hranaError, error) {
	text, err := s.resolveSQL(st.SQL, st.SQLID)
	if err != nil {
		return sqlInfo{}, nil, nil, nil, err
	}
	s.log.Debug("executing", "sql", text)
	info, err := analyzeSQL(text)
	if err != nil {
		return info, nil, nil, &hranaError{Message: err.Error(), Code: "SQLITE_UNKNOWN"}, nil
	}
	switch {
	case info.count == 0:
		return info, nil, nil, &hranaError{Message: "SQL string does not contain any statement", Code: "SQL_NO_STATEMENT"}, nil
	case info.count > 1:
		return info, nil, nil, &hranaError{Message: "SQL string contains more than one statement", Code: "SQL_MANY_STATEMENTS"}, nil
	}
	vals, herr := bindArgs(info.params, st.Args, st.NamedArgs)
	if herr != nil {
		return info, nil, nil, herr, nil
	}
	var args []any
	var restore func(string) string
	info.text, args, restore = info.driverArgs(vals, false)
	return info, args, restore, nil, nil
}

func argsError(format string, a ...any) *hranaError {
	return &hranaError{Message: "Arguments do not match SQL parameters: " + fmt.Sprintf(format, a...), Code: "ARGS_INVALID"}
}

// bindArgs matches arguments to parameters as sqld does: nameless and ?NNN
// parameters take positional arguments, named ones take named arguments,
// whose prefix may be omitted. It returns one value per parameter.
func bindArgs(params []string, args []hranaValue, named []hranaNamedArg) ([]any, *hranaError) {
	if len(args) > 0 && len(named) > 0 {
		return nil, &hranaError{Message: "Specifying both positional and named arguments is not supported", Code: "ARGS_BOTH_POSITIONAL_AND_NAMED"}
	}
	if given := len(args) + len(named); given > len(params) {
		return nil, argsError("too many parameters, expected %d found %d", len(params), given)
	}
	out := make([]any, len(params))
	for i, name := range params {
		positional := name == "" || name[0] == '?'
		if positional && len(named) == 0 {
			if i >= len(args) {
				return nil, argsError("value for parameter %d not found", i+1)
			}
			out[i] = args[i].v
			continue
		}
		v, ok := lookupNamed(named, name)
		if !ok {
			if name == "" {
				return nil, argsError("value for parameter %d not found", i+1)
			}
			return nil, argsError("value for parameter %s not found", name)
		}
		out[i] = v
	}
	return out, nil
}

func lookupNamed(named []hranaNamedArg, name string) (any, bool) {
	if name == "" {
		return nil, false
	}
	for _, a := range named {
		if a.Name == name {
			return a.Value.v, true
		}
	}
	for _, a := range named {
		if len(name) > 1 && a.Name == name[1:] && name[0] != '?' {
			return a.Value.v, true
		}
	}
	return nil, false
}

type stmtSummary struct {
	rowsRead     int64
	affectedRows int64
	lastRowid    *int64
}

// execStmt runs one statement, reporting its columns once and then each
// row (unless want_rows is false). vals passed to onRow are reused.
func (s *session) execStmt(ctx context.Context, st *hranaStmt, onCols func([]hranaCol) error, onRow func(vals []any) error) (stmtSummary, *hranaError, error) {
	var sum stmtSummary
	info, args, restore, herr, err := s.prepareStmt(st)
	if herr != nil || err != nil {
		return sum, herr, err
	}
	rows, err := s.conn.QueryContext(ctx, info.text, args...)
	if err != nil {
		return sum, sqliteError(err), nil
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return sum, sqliteError(err), nil
	}
	if err := onCols(columns(types, restore)); err != nil {
		return sum, nil, err
	}

	wantRows := st.WantRows == nil || *st.WantRows
	vals := make([]any, len(types))
	ptrs := make([]any, len(types))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return sum, sqliteError(err), nil
		}
		sum.rowsRead++
		if wantRows {
			if err := onRow(vals); err != nil {
				return sum, nil, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return sum, sqliteError(err), nil
	}
	if err := rows.Close(); err != nil {
		return sum, sqliteError(err), nil
	}

	// Like sqld: changes() only for DML (trigger rows excluded), and a
	// last_insert_rowid for every INSERT, even one that inserted nothing.
	if !info.explain {
		switch info.kind {
		case "INSERT", "REPLACE", "UPDATE", "DELETE":
			var rowid int64
			if err := s.conn.QueryRowContext(ctx, "SELECT changes(), last_insert_rowid()").Scan(&sum.affectedRows, &rowid); err != nil {
				return sum, sqliteError(err), nil
			}
			if info.kind == "INSERT" || info.kind == "REPLACE" {
				sum.lastRowid = &rowid
			}
		}
	}
	return sum, nil, nil
}

func columns(types []*sql.ColumnType, restore func(string) string) []hranaCol {
	cols := make([]hranaCol, len(types))
	for i, ct := range types {
		cols[i].Name = restore(ct.Name())
		if decl := ct.DatabaseTypeName(); decl != "" {
			cols[i].Decltype = &decl
		}
	}
	return cols
}

// execCollect runs a statement and returns its whole result.
func (s *session) execCollect(ctx context.Context, st *hranaStmt) (*hranaStmtResult, *hranaError, error) {
	start := time.Now()
	res := &hranaStmtResult{Cols: []hranaCol{}, Rows: [][]outValue{}}
	sum, herr, err := s.execStmt(ctx, st,
		func(cols []hranaCol) error { res.Cols = cols; return nil },
		func(vals []any) error {
			row := make([]outValue, len(vals))
			for i, v := range vals {
				row[i] = outValue{v}
			}
			res.Rows = append(res.Rows, row)
			return nil
		})
	if herr != nil || err != nil {
		return nil, herr, err
	}
	res.AffectedRowCount = sum.affectedRows
	if sum.lastRowid != nil {
		id := strconv.FormatInt(*sum.lastRowid, 10)
		res.LastInsertRowid = &id
	}
	res.RowsRead = sum.rowsRead
	res.RowsWritten = sum.affectedRows
	res.QueryDurationMs = float64(time.Since(start).Nanoseconds()) / 1e6
	return res, nil, nil
}

// validateBatch checks conditions and resolves every statement's SQL, so
// that a cursor can report protocol errors before streaming anything.
func (s *session) validateBatch(b *hranaBatch) error {
	for i := range b.Steps {
		if c := b.Steps[i].Condition; c != nil {
			if err := c.validate(i); err != nil {
				return err
			}
		}
		if _, err := s.resolveSQL(b.Steps[i].Stmt.SQL, b.Steps[i].Stmt.SQLID); err != nil {
			return err
		}
	}
	return nil
}

// runBatch executes the steps whose condition holds. exec reports whether
// the step succeeded.
func (s *session) runBatch(b *hranaBatch, exec func(i int, st *hranaStmt) (bool, error)) error {
	if err := s.validateBatch(b); err != nil {
		return err
	}
	outcome := make([]stepOutcome, len(b.Steps))
	for i := range b.Steps {
		if c := b.Steps[i].Condition; c != nil && !s.evalCond(c, outcome) {
			continue
		}
		ok, err := exec(i, &b.Steps[i].Stmt)
		if err != nil {
			return err
		}
		outcome[i] = stepFailed
		if ok {
			outcome[i] = stepSucceeded
		}
	}
	return nil
}

type stepOutcome int

const (
	stepSkipped stepOutcome = iota
	stepSucceeded
	stepFailed
)

func (s *session) evalCond(c *hranaCond, outcome []stepOutcome) bool {
	switch c.Type {
	case "ok":
		return outcome[*c.Step] == stepSucceeded
	case "error":
		// The spec says false for a skipped step; sqld says true, and clients
		// are tested against sqld.
		return outcome[*c.Step] != stepSucceeded
	case "not":
		return !s.evalCond(c.Cond, outcome)
	case "and":
		for i := range c.Conds {
			if !s.evalCond(&c.Conds[i], outcome) {
				return false
			}
		}
		return true
	case "or":
		for i := range c.Conds {
			if s.evalCond(&c.Conds[i], outcome) {
				return true
			}
		}
		return false
	case "is_autocommit":
		return !s.inTx()
	}
	return false
}

// describe reports a statement's parameters and columns without running it.
// EXPLAIN prepares it, which surfaces errors and tells whether it writes.
// Columns are only known for SELECT-like statements, read through a
// LIMIT 0 wrapper; for others (e.g. RETURNING) they are left empty.
func (s *session) describe(ctx context.Context, text string) (*hranaDescribeResult, *hranaError) {
	info, err := analyzeSQL(text)
	if err != nil {
		return nil, &hranaError{Message: err.Error(), Code: "SQLITE_UNKNOWN"}
	}
	res := &hranaDescribeResult{Params: make([]hranaDescribeParam, len(info.params)), Cols: []hranaCol{}, IsExplain: info.explain, IsReadonly: true}
	if info.count == 0 {
		return res, nil // as sqld: nothing to describe is no error
	}
	for i, name := range info.params {
		if name != "" {
			res.Params[i].Name = &name
		}
	}
	nulls := make([]any, len(info.params))
	body, bodyArgs, _ := info.driverArgs(nulls, true)
	text, args, restore := info.driverArgs(nulls, false)

	rows, err := s.conn.QueryContext(ctx, "EXPLAIN "+body, bodyArgs...)
	if err != nil {
		return nil, sqliteError(err)
	}
	for rows.Next() {
		var addr, p1, p2, p3, p5 int64
		var opcode string
		var p4, comment any
		if err := rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
			rows.Close()
			return nil, sqliteError(err)
		}
		// The opcodes that clear the readOnly flag in SQLite's resolveP2Values.
		switch opcode {
		case "Transaction":
			if p2 != 0 {
				res.IsReadonly = false
			}
		case "Checkpoint", "Vacuum", "JournalMode":
			res.IsReadonly = false
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, sqliteError(err)
	}

	var colsSQL string
	switch {
	case info.explain:
		colsSQL = text // only lists the program
	case res.IsReadonly && (info.kind == "SELECT" || info.kind == "VALUES"):
		colsSQL = "SELECT * FROM (" + text + ") LIMIT 0"
	}
	if colsSQL != "" {
		if rows, err := s.conn.QueryContext(ctx, colsSQL, args...); err == nil {
			if types, err := rows.ColumnTypes(); err == nil {
				res.Cols = undoDedup(columns(types, restore))
			}
			rows.Close()
		}
	}
	return res, nil
}

// undoDedup restores the duplicate column names that the LIMIT 0 wrapper's
// subquery renamed: SELECT a, a reads back as "a", "a:1".
func undoDedup(cols []hranaCol) []hranaCol {
	seen := map[string]bool{}
	for i := range cols {
		name := cols[i].Name
		if j := strings.LastIndexByte(name, ':'); j > 0 && seen[name[:j]] {
			if _, err := strconv.Atoi(name[j+1:]); err == nil {
				cols[i].Name = name[:j]
			}
		}
		seen[cols[i].Name] = true
	}
	return cols
}
