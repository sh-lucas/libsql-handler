package libsqlhandler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// request is the v1 body: {"statements": ["SELECT 1", {"q": "...", "params": [...] | {...}}]}.
type request struct {
	Statements []statement `json:"statements"`
}

type statement struct {
	query string
	args  []any
}

func (s *statement) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &s.query); err == nil {
		return nil
	}
	var obj struct {
		Q      string          `json:"q"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	s.query = obj.Q
	if len(obj.Params) == 0 {
		return nil
	}

	// UseNumber keeps integers as INTEGER and exact beyond 2^53, instead of float64.
	var params any
	dec := json.NewDecoder(bytes.NewReader(obj.Params))
	dec.UseNumber()
	if err := dec.Decode(&params); err != nil {
		return fmt.Errorf("invalid params: %w", err)
	}
	switch p := params.(type) {
	case nil:
	case []any:
		for _, v := range p {
			arg, err := sqlValue(v)
			if err != nil {
				return err
			}
			s.args = append(s.args, arg)
		}
	case map[string]any:
		// database/sql binds by bare name, so ":x" and "@x" would silently share one value.
		seen := make(map[string]string, len(p))
		for k, v := range p {
			name := strings.TrimLeft(k, ":@$")
			if prev, dup := seen[name]; dup {
				return fmt.Errorf("invalid params: %q and %q refer to the same name %q", prev, k, name)
			}
			seen[name] = k
			arg, err := sqlValue(v)
			if err != nil {
				return err
			}
			s.args = append(s.args, sql.Named(name, arg))
		}
	default:
		return errors.New("invalid params: must be an array or an object")
	}
	return nil
}

func sqlValue(v any) (any, error) {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, nil
		}
		f, _ := x.Float64()
		return f, nil
	case map[string]any:
		// sqld's blob form, the same one used in results.
		if b64, ok := x["base64"].(string); ok && len(x) == 1 {
			b, err := base64.StdEncoding.DecodeString(b64)
			if err != nil {
				return nil, fmt.Errorf("invalid base64 param: %w", err)
			}
			return b, nil
		}
		return nil, errors.New(`invalid param: objects other than {"base64": "..."} are not supported`)
	case []any:
		return nil, errors.New("invalid param: arrays are not supported")
	}
	return v, nil
}

type results struct {
	Columns         []string `json:"columns"`
	Rows            [][]any  `json:"rows"`
	RowsRead        int64    `json:"rows_read"`
	RowsWritten     int64    `json:"rows_written"`
	QueryDurationMs float64  `json:"query_duration_ms"`
}

type statementResult struct {
	Results *results `json:"results,omitempty"`
	Error   string   `json:"error,omitempty"`
}

// runBatch executes statements in order. Like sqld, it stops at the first
// error and reports null for the statements it skipped.
func (h *LibSQLHandler) runBatch(ctx context.Context, conn *sql.Conn, stmts []statement) []*statementResult {
	out := make([]*statementResult, len(stmts))
	for i, st := range stmts {
		h.log.Debug("executing", "sql", st.query)
		res, err := execute(ctx, conn, st)
		if err != nil {
			out[i] = &statementResult{Error: err.Error()}
			break
		}
		out[i] = &statementResult{Results: res}
	}
	return out
}

// execute runs any statement through Query, so RETURNING and plain DML need no
// SQL sniffing; rows_written comes from the total_changes() delta.
func execute(ctx context.Context, conn *sql.Conn, st statement) (*results, error) {
	start := time.Now()
	before, err := totalChanges(ctx, conn)
	if err != nil {
		return nil, err
	}

	rows, err := conn.QueryContext(ctx, st.query, st.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	out := [][]any{}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make([]any, len(cols))
		for i, v := range vals {
			row[i] = jsonValue(v)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	after, err := totalChanges(ctx, conn)
	if err != nil {
		return nil, err
	}
	if cols == nil {
		cols = []string{}
	}
	return &results{
		Columns:         cols,
		Rows:            out,
		RowsRead:        int64(len(out)),
		RowsWritten:     after - before,
		QueryDurationMs: float64(time.Since(start).Nanoseconds()) / 1e6,
	}, nil
}

func totalChanges(ctx context.Context, conn *sql.Conn) (int64, error) {
	var n int64
	err := conn.QueryRowContext(ctx, "SELECT total_changes()").Scan(&n)
	return n, err
}

// SQLite's own text format; the driver parses DATE/DATETIME/TIMESTAMP columns
// into time.Time and the original text is not recoverable.
const sqliteTime = "2006-01-02 15:04:05.999999999"

func jsonValue(v any) any {
	switch x := v.(type) {
	case []byte: // BLOB; drivers return TEXT as string
		return map[string]string{"base64": base64.StdEncoding.EncodeToString(x)}
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return nil // not representable in JSON
		}
	case time.Time:
		if _, offset := x.Zone(); offset == 0 {
			return x.Format(sqliteTime)
		}
		return x.Format(sqliteTime + "-07:00")
	}
	return v
}
