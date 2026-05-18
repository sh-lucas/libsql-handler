package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Statement represents a SQL query with optional parameters.
// It implements a custom JSON unmarshaler to support both simple strings
// and structured parameter objects sent by DBeaver.
type Statement struct {
	Query       string
	Params      []interface{}
	NamedParams map[string]interface{}
}

// MarshalJSON implements json.Marshaler.
func (s Statement) MarshalJSON() ([]byte, error) {
	// If no parameters, serialize as a plain string to match DBeaver's optimized format
	if len(s.Params) == 0 && len(s.NamedParams) == 0 {
		return json.Marshal(s.Query)
	}

	// Otherwise, serialize as an object with "q" and "params"
	var params interface{}
	if len(s.Params) > 0 {
		params = s.Params
	} else if len(s.NamedParams) > 0 {
		params = s.NamedParams
	}

	return json.Marshal(struct {
		Q      string      `json:"q"`
		Params interface{} `json:"params"`
	}{
		Q:      s.Query,
		Params: params,
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *Statement) UnmarshalJSON(data []byte) error {
	// 1. Try parsing as a plain JSON string (simple query)
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		s.Query = str
		return nil
	}

	// 2. Try parsing as a structured query object
	var obj struct {
		Q      string          `json:"q"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	s.Query = obj.Q

	if len(obj.Params) > 0 {
		// Determine if params is a JSON array [] or object {}
		trimmed := strings.TrimSpace(string(obj.Params))
		if len(trimmed) > 0 {
			if trimmed[0] == '[' {
				var list []interface{}
				if err := json.Unmarshal(obj.Params, &list); err != nil {
					return fmt.Errorf("failed to parse positional params: %w", err)
				}
				s.Params = list
			} else if trimmed[0] == '{' {
				var m map[string]interface{}
				if err := json.Unmarshal(obj.Params, &m); err != nil {
					return fmt.Errorf("failed to parse named params: %w", err)
				}
				s.NamedParams = m
			}
		}
	}
	return nil
}

// Request represents the incoming HTTP POST request containing database statements.
type Request struct {
	Statements []Statement `json:"statements"`
}

// Results represents the output columns and rows returned to the client.
type Results struct {
	Columns         []string        `json:"columns"`
	Rows            [][]interface{} `json:"rows"`
	RowsRead        int64           `json:"rows_read"`
	RowsWritten     int64           `json:"rows_written"`
	QueryDurationMs float64         `json:"query_duration_ms"`
}

// StatementResponse represents the execution result or error for a single statement.
type StatementResponse struct {
	Error   string   `json:"error,omitempty"`
	Results *Results `json:"results,omitempty"`
}

// Session represents a stateful connection to the database.
// This is critical because HTTP is stateless, but DBeaver expects transactions
// (e.g. BEGIN, COMMIT) to run on the exact same SQLite connection.
type Session struct {
	ID        string
	Conn      *sql.Conn
	LastUsed  time.Time
	Mutex     sync.Mutex
}

// LibSQLHandler wraps the SQLite database and session map.
type LibSQLHandler struct {
	db             *sql.DB
	sessions       map[string]*Session
	mu             sync.RWMutex
	sessionTimeout time.Duration
}

// NewLibSQLHandler initializes the handler and database connection.
func NewLibSQLHandler(driverName, dataSourceName string) (*LibSQLHandler, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set connection limits
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)

	h := &LibSQLHandler{
		db:             db,
		sessions:       make(map[string]*Session),
		sessionTimeout: 10 * time.Minute,
	}

	// Start background session cleaner
	go h.cleanupIdleSessions()

	return h, nil
}

// Close closes the database connection and active sessions.
func (h *LibSQLHandler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, s := range h.sessions {
		s.Conn.Close()
	}
	h.sessions = make(map[string]*Session)
	return h.db.Close()
}

// generateSessionID creates a unique cryptographically secure random string.
func generateSessionID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("sess_%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// getOrCreateSession retrieves an existing session from the "session" cookie,
// or creates a new one with a dedicated database connection.
func (h *LibSQLHandler) getOrCreateSession(w http.ResponseWriter, r *http.Request) (*Session, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var sessionID string
	cookie, err := r.Cookie("session")
	if err == nil {
		sessionID = cookie.Value
		if s, ok := h.sessions[sessionID]; ok {
			s.LastUsed = time.Now()
			return s, nil
		}
	}

	// Create a new session if none exists or it has expired
	sessionID = generateSessionID()
	ctx := context.Background()
	conn, err := h.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate dedicated connection: %w", err)
	}

	// Configure connection-scoped pragmas for SQLite
	_, _ = conn.ExecContext(ctx, "PRAGMA journal_mode = WAL")
	_, _ = conn.ExecContext(ctx, "PRAGMA busy_timeout = 5000")

	s := &Session{
		ID:       sessionID,
		Conn:     conn,
		LastUsed: time.Now(),
	}
	h.sessions[sessionID] = s

	// Set the session cookie.
	// Since DBeaver client has a CookieManager, it will automatically
	// preserve and send this cookie back to keep session stickiness!
	http.SetCookie(w, &http.Cookie{
		Name:     "session",
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})

	log.Printf("[Session] Created new session %s", sessionID)
	return s, nil
}

// cleanupIdleSessions periodically purges sessions that have exceeded h.sessionTimeout.
func (h *LibSQLHandler) cleanupIdleSessions() {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		h.mu.Lock()
		now := time.Now()
		for id, s := range h.sessions {
			if now.Sub(s.LastUsed) > h.sessionTimeout {
				log.Printf("[Session] Purging idle session %s", id)
				s.Conn.Close()
				delete(h.sessions, id)
			}
		}
		h.mu.Unlock()
	}
}

// ServeHTTP routes and processes HTTP requests.
func (h *LibSQLHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Enable CORS
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Route: GET /version
	if r.Method == http.MethodGet && r.URL.Path == "/version" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		// Must match VERSION_PATTERN "(\\w+)\\s+([0-9.]+)\\s+(.+)" in DBeaver driver
		w.Write([]byte("sqld 0.20.0 main\n"))
		return
	}

	// Route: POST /
	if r.Method == http.MethodPost && (r.URL.Path == "/" || r.URL.Path == "") {
		h.handlePostQuery(w, r)
		return
	}

	http.Error(w, "Not Found", http.StatusNotFound)
}

func (h *LibSQLHandler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	s, err := h.getOrCreateSession(w, r)
	if err != nil {
		log.Printf("[Error] Failed to resolve session: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Lock the session connection so that multiple statements in a single batch
	// or concurrent requests from the same session execute sequentially.
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[Error] Failed to decode JSON request: %v", err)
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	responses := make([]StatementResponse, len(req.Statements))
	ctx := r.Context()

	for i, stmt := range req.Statements {
		startTime := time.Now()
		var resp StatementResponse

		// Bind parameters (positional or named)
		var args []interface{}
		if len(stmt.Params) > 0 {
			args = stmt.Params
		} else if len(stmt.NamedParams) > 0 {
			for k, v := range stmt.NamedParams {
				args = append(args, sql.Named(k, v))
			}
		}

		log.Printf("[Query] Executing Statement %d: Query=%q, Args=%+v", i, stmt.Query, args)

		// Execute statement
		results, err := executeSQL(ctx, s.Conn, stmt.Query, args)
		if err != nil {
			resp.Error = err.Error()
			log.Printf("[Query] Statement %d failed with error: %v", i, err)
		} else {
			results.QueryDurationMs = float64(time.Since(startTime).Nanoseconds()) / 1e6
			resp.Results = results
			log.Printf("[Query] Statement %d succeeded. Columns=%+v, RowsCount=%d, RowsWritten=%d", i, results.Columns, len(results.Rows), results.RowsWritten)
		}
		responses[i] = resp
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(responses); err != nil {
		log.Printf("[Error] Failed to encode response: %v", err)
	}
}

// isQuery checks if the query is a SELECT, PRAGMA, WITH, EXPLAIN or other read-only queries
func isQuery(query string) bool {
	q := strings.TrimSpace(strings.ToLower(query))
	for {
		if strings.HasPrefix(q, "--") {
			if idx := strings.Index(q, "\n"); idx != -1 {
				q = strings.TrimSpace(q[idx:])
				continue
			}
		}
		if strings.HasPrefix(q, "/*") {
			if idx := strings.Index(q, "*/"); idx != -1 {
				q = strings.TrimSpace(q[idx+2:])
				continue
			}
		}
		break
	}
	return strings.HasPrefix(q, "select") ||
		strings.HasPrefix(q, "pragma") ||
		strings.HasPrefix(q, "with") ||
		strings.HasPrefix(q, "explain") ||
		strings.HasPrefix(q, "show")
}

// safeRowsAffected safely retrieves rows affected, recovering from any driver panic.
func safeRowsAffected(res sql.Result) int64 {
	if res == nil {
		return 0
	}
	defer func() {
		_ = recover()
	}()
	n, err := res.RowsAffected()
	if err != nil {
		return 0
	}
	return n
}

// executeSQL runs a SQL statement on a dedicated connection.
// It uses either QueryContext or ExecContext based on the statement type.
func executeSQL(ctx context.Context, conn *sql.Conn, query string, args []interface{}) (*Results, error) {
	if isQuery(query) {
		rows, err := conn.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		// Prepare scanning arguments
		scanArgs := make([]interface{}, len(cols))
		values := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		resultRows := [][]interface{}{}
		for rows.Next() {
			if err := rows.Scan(scanArgs...); err != nil {
				return nil, err
			}

			rowVals := make([]interface{}, len(cols))
			for i, v := range values {
				switch val := v.(type) {
				case []byte:
					// Strings are often scanned as []byte, decode to UTF-8 string
					rowVals[i] = string(val)
				default:
					rowVals[i] = val
				}
			}
			resultRows = append(resultRows, rowVals)
		}

		if err := rows.Err(); err != nil {
			return nil, err
		}

		return &Results{
			Columns:     cols,
			Rows:        resultRows,
			RowsRead:    int64(len(resultRows)),
			RowsWritten: 0,
		}, nil
	}

	// For mutation/DDL queries, execute using ExecContext
	res, err := conn.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	rowsWritten := safeRowsAffected(res)
	return &Results{
		Columns:     []string{},
		Rows:        [][]interface{}{},
		RowsRead:    0,
		RowsWritten: rowsWritten,
	}, nil
}


