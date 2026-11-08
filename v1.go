package libsqlhandler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"
)

// The legacy v1 API (docs: libsql/docs/HTTP_V1_SPEC.md), still the only one
// DBeaver's LibSQL JDBC driver speaks. Its sessions are kept by a cookie.

// serveV1 runs a v1 batch on the client's cookie session.
func (h *LibSQLHandler) serveV1(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(w, r, writeError) {
		return
	}
	// Read the body before entering, so Close never waits on a slow client.
	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	ctx, done, ok := h.enter(w, r, writeError)
	if !ok {
		return
	}
	defer done()

	s, err := h.acquire(ctx, w, r)
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, errSessionLost) {
			status = http.StatusConflict
			// Drop the cookie so the client's next request starts fresh, while
			// any other request still carrying it keeps getting 409.
			http.SetCookie(w, &http.Cookie{Name: sessionCookie, Path: "/", MaxAge: -1})
		}
		writeError(w, status, err.Error())
		return
	}
	results := runBatch(ctx, h.log, s.conn, req.Statements)
	h.release(s)

	body, err := json.Marshal(results)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "encoding response: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(body)
}

// writeError replies with a JSON body the DBeaver driver surfaces as the SQL error.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

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
func runBatch(ctx context.Context, log *slog.Logger, conn *sql.Conn, stmts []statement) []*statementResult {
	out := make([]*statementResult, len(stmts))
	for i, st := range stmts {
		log.Debug("executing", "sql", st.query)
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
		return formatTime(x)
	}
	return v
}

// formatTime renders a time the driver parsed from a DATE/DATETIME/TIMESTAMP
// column back in SQLite's format.
func formatTime(t time.Time) string {
	if _, offset := t.Zone(); offset == 0 {
		return t.Format(sqliteTime)
	}
	return t.Format(sqliteTime + "-07:00")
}
