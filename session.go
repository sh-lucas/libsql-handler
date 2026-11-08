package libsqlhandler

import (
	"cmp"
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const sessionCookie = "session"

// lostRetention is how long a client is told its expired transaction was
// rolled back. Only sessions dropped mid-transaction are remembered.
const lostRetention = 24 * time.Hour

var (
	errClosed          = errors.New("libsql handler is closed")
	errTooManySessions = errors.New("too many open sessions, try again later")
	errSessionLost     = errors.New("session expired with an open transaction, which was rolled back; nothing in this request ran, retry to start a new session")
)

// session is a client's sticky connection: a v1 cookie session or a Hrana
// stream. mu is held for the whole request, so a session runs one request at
// a time and is never swept while in use.
type session struct {
	mu       sync.Mutex
	key      string    // its key in h.sessions: the cookie, or the current baton; written under mu and h.mu
	conn     *sql.Conn // nil once discarded
	lastUsed atomic.Int64
	reused   atomic.Bool      // not one-shot: the cookie came back, or it is a Hrana stream
	sqls     map[int32]string // Hrana store_sql texts, scoped to the stream
	log      *slog.Logger
}

func (s *session) touch() { s.lastUsed.Store(time.Now().UnixNano()) }

func (s *session) expired(now time.Time, timeout time.Duration) bool {
	return now.UnixNano()-s.lastUsed.Load() > int64(timeout)
}

// inTx reports whether the connection has an open transaction. It probes
// with plain BEGIN/ROLLBACK, which works on any driver; when unsure it says
// yes. Caller holds s.mu.
func (s *session) inTx() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// The probe's ROLLBACK ends a transaction, which resets this pragma.
	var deferFK int
	_ = s.conn.QueryRowContext(ctx, "PRAGMA defer_foreign_keys").Scan(&deferFK)
	if _, err := s.conn.ExecContext(ctx, "BEGIN"); err != nil {
		return true
	}
	_, _ = s.conn.ExecContext(ctx, "ROLLBACK")
	if deferFK != 0 {
		_, _ = s.conn.ExecContext(ctx, "PRAGMA defer_foreign_keys = ON")
	}
	return false
}

// discard closes the connection instead of returning it to the pool: an open
// transaction must die with the session, not leak to the next pool user.
// Caller holds s.mu.
func (s *session) discard() {
	if s.conn == nil {
		return
	}
	_ = s.conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = s.conn.Close()
	s.conn = nil
}

// acquire returns the caller's session, locked, creating one if needed.
func (h *LibSQLHandler) acquire(ctx context.Context, w http.ResponseWriter, r *http.Request) (*session, error) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		s, err := h.lookup(c.Value)
		if err != nil {
			return nil, err
		}
		if s != nil {
			s.mu.Lock()
			if s.conn != nil {
				return s, nil
			}
			// Discarded while we waited (handler closed): fall through to a fresh session.
			s.mu.Unlock()
		}
	}
	s, err := h.create(ctx)
	if err != nil {
		return nil, err
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    s.key,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
	return s, nil
}

func (h *LibSQLHandler) release(s *session) {
	s.touch()
	s.mu.Unlock()
}

// lookup finds a live session. If the session was dropped with a transaction
// open, every request still naming it gets errSessionLost (until
// lostRetention): running the rest of the client's transaction on a fresh
// session would autocommit half of it.
func (h *LibSQLHandler) lookup(id string) (*session, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.lost[id]; ok {
		return nil, errSessionLost
	}
	s := h.sessions[id]
	if s == nil {
		return nil, nil
	}
	if s.expired(time.Now(), h.sessionTimeout) && s.mu.TryLock() {
		hadTx := h.dropLocked(id, s)
		s.mu.Unlock()
		if hadTx {
			return nil, errSessionLost
		}
		return nil, nil
	}
	s.reused.Store(true)
	s.touch()
	return s, nil
}

// dropLocked discards an expired session, remembering it if it had a
// transaction open. Caller holds h.mu and s.mu.
func (h *LibSQLHandler) dropLocked(id string, s *session) (hadTx bool) {
	hadTx = s.inTx()
	if hadTx {
		h.lost[id] = time.Now()
	}
	s.discard()
	delete(h.sessions, id)
	return hadTx
}

// create opens a session on its own pool connection and returns it locked.
func (h *LibSQLHandler) create(ctx context.Context) (*session, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, errClosed
	}
	if len(h.sessions)+h.pending >= h.sessionLimit() {
		h.sweepLocked(time.Now())
		if len(h.sessions)+h.pending >= h.sessionLimit() && !h.evictOneShotLocked() {
			h.mu.Unlock()
			return nil, errTooManySessions
		}
	}
	h.pending++
	h.mu.Unlock()

	// Wait for a pool connection without holding h.mu, so other sessions keep working.
	s := &session{log: h.log}
	ctx, cancel := context.WithTimeout(ctx, acquireTimeout)
	defer cancel()
	conn, err := h.db.Conn(ctx)
	if err == nil {
		s.conn = conn
		_, err = conn.ExecContext(ctx, "PRAGMA busy_timeout = 5000")
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.pending--
	if err == nil && h.closed {
		err = errClosed
	}
	if err != nil {
		s.discard()
		if errors.Is(err, errClosed) {
			return nil, err
		}
		return nil, fmt.Errorf("allocating connection: %w", err)
	}

	s.key = rand.Text()
	s.touch()
	s.mu.Lock()
	h.sessions[s.key] = s
	return s, nil
}

// rekey moves a session to a fresh random key and returns it, or nil if the
// session is no longer registered (the handler closed). Caller holds s.mu.
func (h *LibSQLHandler) rekey(s *session) *string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sessions[s.key] != s {
		return nil
	}
	delete(h.sessions, s.key)
	s.key = rand.Text()
	h.sessions[s.key] = s
	key := s.key
	return &key
}

// closeSession unregisters and discards a session, rolling back any open
// transaction. Caller holds s.mu.
func (h *LibSQLHandler) closeSession(s *session) {
	h.mu.Lock()
	if h.sessions[s.key] == s {
		delete(h.sessions, s.key)
	}
	h.mu.Unlock()
	s.discard()
}

// sessionLimit keeps one pool connection free for the WAL checkpoint and any
// other user of the db, since each session pins one connection.
func (h *LibSQLHandler) sessionLimit() int {
	limit := h.maxSessions
	if open := h.db.Stats().MaxOpenConnections; open > 0 {
		limit = min(limit, max(open-1, 1))
	}
	return limit
}

// sweepLocked drops idle sessions past the timeout, and forgets lost ones
// whose client never came back within lostRetention. Caller holds h.mu.
func (h *LibSQLHandler) sweepLocked(now time.Time) {
	for id, s := range h.sessions {
		if s.expired(now, h.sessionTimeout) && s.mu.TryLock() {
			h.dropLocked(id, s)
			s.mu.Unlock()
		}
	}
	for id, at := range h.lost {
		if now.Sub(at) > lostRetention {
			delete(h.lost, id)
		}
	}
}

// evictOneShotLocked frees a slot by dropping the oldest idle session whose
// client never sent the cookie back (curl, scripts, health checks) and that
// has no open transaction: a client's first request may well be BEGIN.
// Sessions of cookie-keeping clients like DBeaver are only dropped by the
// timeout. Caller holds h.mu.
func (h *LibSQLHandler) evictOneShotLocked() bool {
	type candidate struct {
		id string
		s  *session
	}
	var oneShot []candidate
	for id, s := range h.sessions {
		if !s.reused.Load() {
			oneShot = append(oneShot, candidate{id, s})
		}
	}
	slices.SortFunc(oneShot, func(a, b candidate) int {
		return cmp.Compare(a.s.lastUsed.Load(), b.s.lastUsed.Load())
	})
	for _, c := range oneShot {
		if c.s.mu.TryLock() {
			if c.s.inTx() {
				c.s.mu.Unlock()
				continue
			}
			c.s.discard()
			c.s.mu.Unlock()
			delete(h.sessions, c.id)
			return true
		}
	}
	return false
}

func (h *LibSQLHandler) sweepSessions() {
	defer h.wg.Done()
	ticker := time.NewTicker(min(max(h.sessionTimeout/2, time.Millisecond), time.Minute))
	defer ticker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case now := <-ticker.C:
			h.mu.Lock()
			h.sweepLocked(now)
			h.mu.Unlock()
		}
	}
}
