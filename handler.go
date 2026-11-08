// Package libsqlhandler serves a SQLite *sql.DB over the libSQL (sqld) HTTP
// protocols: Hrana over HTTP (v2 and v3, JSON), spoken by the official libSQL
// clients, and the legacy v1 API spoken by DBeaver's LibSQL JDBC driver.
//
// Each Hrana stream, and each v1 client (via a "session" cookie), is pinned
// to its own connection, so transactions spanning several requests behave
// like they would on a local connection.
package libsqlhandler

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultSessionTimeout = 10 * time.Minute
	defaultMaxSessions    = 64
	// acquireTimeout bounds every wait for a pool connection, so a saturated
	// pool turns into a 503 instead of a hung request.
	acquireTimeout  = 5 * time.Second
	checkpointDelay = time.Second
)

// LibSQLHandler is an http.Handler serving the libSQL HTTP protocols.
type LibSQLHandler struct {
	db             *sql.DB
	log            *slog.Logger
	sessionTimeout time.Duration
	maxSessions    int

	mu       sync.Mutex
	sessions map[string]*session
	lost     map[string]time.Time // sessions dropped with an open transaction
	pending  int                  // sessions being created, counted against the limit
	closed   bool
	requests sync.WaitGroup // in-flight queries, awaited by Close

	checkpoint chan struct{}
	ctx        context.Context // canceled by Close
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	closeOnce  sync.Once
}

// Option configures a LibSQLHandler.
type Option func(*LibSQLHandler)

// WithSessionTimeout sets how long an idle session (and any transaction it has
// open) is kept before it is rolled back and dropped. Default: 10 minutes.
func WithSessionTimeout(d time.Duration) Option {
	return func(h *LibSQLHandler) {
		if d > 0 {
			h.sessionTimeout = d
		}
	}
}

// WithMaxSessions caps concurrent sessions. Each session pins one pool
// connection, so the effective cap is also bounded by db.SetMaxOpenConns.
// Default: 64.
func WithMaxSessions(n int) Option {
	return func(h *LibSQLHandler) {
		if n > 0 {
			h.maxSessions = n
		}
	}
}

// WithLogger sets the logger. Executed SQL is logged at debug level.
// Default: logs are discarded.
func WithLogger(l *slog.Logger) Option {
	return func(h *LibSQLHandler) {
		if l != nil {
			h.log = l
		}
	}
}

// New creates a handler over an already open db. The caller owns db and must
// close it after Close.
func New(db *sql.DB, opts ...Option) *LibSQLHandler {
	h := &LibSQLHandler{
		db:             db,
		log:            slog.New(slog.DiscardHandler),
		sessionTimeout: defaultSessionTimeout,
		maxSessions:    defaultMaxSessions,
		sessions:       make(map[string]*session),
		lost:           make(map[string]time.Time),
		checkpoint:     make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(h)
	}
	h.ctx, h.cancel = context.WithCancel(context.Background())

	// WAL lets other sessions keep reading while one holds a write transaction.
	ctx, cancel := context.WithTimeout(h.ctx, acquireTimeout)
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
		h.log.Warn("enabling WAL failed", "err", err)
	}
	cancel()

	h.wg.Add(2)
	go h.sweepSessions()
	go h.checkpointWAL()
	return h
}

// Close interrupts in-flight queries, rolls back and closes every session,
// and stops background work, returning once all of it has finished. Safe to
// call more than once; requests arriving afterwards get 503.
func (h *LibSQLHandler) Close() error {
	h.closeOnce.Do(func() {
		h.mu.Lock()
		h.closed = true
		sessions := h.sessions
		h.sessions = make(map[string]*session)
		h.mu.Unlock()

		h.cancel() // interrupts running queries and pending connection waits
		for _, s := range sessions {
			s.mu.Lock()
			s.discard()
			s.mu.Unlock()
		}
		h.requests.Wait()
		h.wg.Wait()
	})
	return nil
}

// ServeHTTP implements http.Handler.
func (h *LibSQLHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if !strings.HasPrefix(path, "/") {
		// Tolerate http.StripPrefix("/prefix/", h), which leaves "v3/pipeline" or "".
		path = "/" + path
	}
	switch r.Method + " " + path {
	case "GET /version":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		// Must match VERSION_PATTERN "(\\w+)\\s+([0-9.]+)\\s+(.+)" in the DBeaver driver.
		_, _ = w.Write([]byte("sqld 0.20.0 main\n"))
	case "POST /":
		h.serveV1(w, r)
	case "GET /v2", "GET /v3":
		// Hrana version probes. No /v3-protobuf: clients then fall back to JSON.
		w.WriteHeader(http.StatusOK)
	case "POST /v2/pipeline", "POST /v3/pipeline":
		h.serveHranaPipeline(w, r)
	case "POST /v3/cursor":
		h.serveHranaCursor(w, r)
	default:
		http.Error(w, "Not Found", http.StatusNotFound)
	}
}

// crossOrigin rejects browser requests from other origins, including
// preflight-free text/plain POSTs. Non-browser clients like DBeaver send
// neither Origin nor Sec-Fetch-Site and pass.
var crossOrigin = http.NewCrossOriginProtection()

// sameOrigin rejects cross-origin browser requests, before their body is read.
func sameOrigin(w http.ResponseWriter, r *http.Request, fail func(http.ResponseWriter, int, string)) bool {
	if err := crossOrigin.Check(r); err != nil {
		fail(w, http.StatusForbidden, err.Error())
		return false
	}
	return true
}

// enter admits a request, rejecting requests after Close. Call it once the
// body is read, so Close never waits on a slow client. The returned context
// is canceled when the client goes away or the handler closes; done must be
// called when finished.
func (h *LibSQLHandler) enter(w http.ResponseWriter, r *http.Request, fail func(http.ResponseWriter, int, string)) (context.Context, func(), bool) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		fail(w, http.StatusServiceUnavailable, errClosed.Error())
		return nil, nil, false
	}
	h.requests.Add(1)
	h.mu.Unlock()

	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(h.ctx, cancel)
	return ctx, func() {
		stop()
		cancel()
		h.requests.Done()
		select {
		case h.checkpoint <- struct{}{}:
		default:
		}
	}, true
}

// checkpointWAL folds the WAL back into the main db file after
// checkpointDelay of silence, keeping the -wal file from growing unbounded.
func (h *LibSQLHandler) checkpointWAL() {
	defer h.wg.Done()
	timer := time.NewTimer(checkpointDelay)
	timer.Stop()
	defer timer.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.checkpoint:
			timer.Reset(checkpointDelay)
		case <-timer.C:
			ctx, cancel := context.WithTimeout(h.ctx, acquireTimeout)
			// Fails with SQLITE_BUSY while a session holds a transaction; the next quiet period retries.
			_, _ = h.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
			cancel()
		}
	}
}
