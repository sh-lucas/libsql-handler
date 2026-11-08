package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	libsqlhandler "github.com/sh-lucas/libsql-handler"
)

func TestTransactionsAreIsolatedPerSession(t *testing.T) {
	e := newEnv(t, 0)
	a, other := e.client(true), e.client(false)
	a.mustExec("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL)")
	a.mustExec("BEGIN", "INSERT INTO accounts VALUES (1, 100.0)")

	if got := a.scalar("SELECT count(*) FROM accounts"); got != 1.0 {
		t.Fatalf("own session must see its uncommitted row, count = %v", got)
	}
	if got := other.scalar("SELECT count(*) FROM accounts"); got != 0.0 {
		t.Fatalf("uncommitted row leaked to another session, count = %v", got)
	}
	a.mustExec("COMMIT")
	if got := other.scalar("SELECT balance FROM accounts WHERE id = 1"); got != 100.0 {
		t.Fatalf("committed row not visible, balance = %v", got)
	}
}

func TestSessionCookie(t *testing.T) {
	e := newEnv(t, 0)
	resp, err := http.Post(e.srv.URL, "application/json", strings.NewReader(`{"statements": ["SELECT 1"]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	cookies := resp.Cookies()
	if len(cookies) != 1 || cookies[0].Name != "session" || len(cookies[0].Value) < 16 || !cookies[0].HttpOnly {
		t.Fatalf("unexpected cookies: %+v", cookies)
	}
}

// Every cookieless request opens a session pinning a connection. They used to
// pile up until the pool ran dry, then the next request blocked forever while
// holding the global lock, freezing every session.
func TestCookielessClientsCannotExhaustThePool(t *testing.T) {
	e := newEnv(t, 3)
	sticky := e.client(true)
	sticky.mustExec("CREATE TABLE t (x)")
	sticky.mustExec("BEGIN", "INSERT INTO t VALUES (1)") // second request: cookie came back

	for i := range 20 {
		if got := e.client(false).scalar("SELECT 1"); got != 1.0 {
			t.Fatalf("request %d: got %v", i, got)
		}
	}
	// The sticky session and its transaction survived the flood.
	sticky.mustExec("INSERT INTO t VALUES (2)", "COMMIT")
	if got := e.client(false).scalar("SELECT count(*) FROM t"); got != 2.0 {
		t.Fatalf("count = %v", got)
	}
}

func TestFullHandlerAnswers503InsteadOfHanging(t *testing.T) {
	e := newEnv(t, 0, libsqlhandler.WithMaxSessions(1))
	a := e.client(true)
	a.mustExec("SELECT 1")
	a.mustExec("SELECT 1") // a is now a returning client, not evictable

	start := time.Now()
	status, body := e.client(false).post(`{"statements": ["SELECT 1"]}`)
	var msg map[string]string
	if status != http.StatusServiceUnavailable || json.Unmarshal([]byte(body), &msg) != nil || msg["error"] == "" {
		t.Fatalf("got %d %s", status, body)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("rejection took %v, should be immediate", time.Since(start))
	}
	if got := a.scalar("SELECT 2"); got != 2.0 {
		t.Fatalf("existing session broken: %v", got)
	}
}

// An expired session used to hand its connection back to the pool with the
// transaction still open: the next user saw uncommitted rows and inherited
// the write lock. Then, the client's next request silently ran on a fresh
// session, autocommitting the rest of its transaction.
func TestExpiredTransactionIsRolledBackAndReported(t *testing.T) {
	// One connection: if the old one were reused, the new session would get it.
	e := newEnv(t, 1, libsqlhandler.WithSessionTimeout(50*time.Millisecond))
	a := e.client(true)
	a.mustExec("CREATE TABLE t (x)")
	a.mustExec("BEGIN", "INSERT INTO t VALUES (1)")
	time.Sleep(200 * time.Millisecond)

	status, body := a.post(`{"statements": ["INSERT INTO t VALUES (2)", "COMMIT"]}`)
	if status != http.StatusConflict || !strings.Contains(body, "rolled back") {
		t.Fatalf("got %d %s", status, body)
	}
	if got := a.scalar("SELECT count(*) FROM t"); got != 0.0 {
		t.Fatalf("expired transaction leaked or the rejected request ran, count = %v", got)
	}
	a.mustExec("BEGIN", "ROLLBACK") // fails if a transaction is still open on this connection
}

func TestExpiredIdleSessionIsReplacedSilently(t *testing.T) {
	e := newEnv(t, 0, libsqlhandler.WithSessionTimeout(50*time.Millisecond))
	a := e.client(true)
	a.mustExec("SELECT 1")
	time.Sleep(200 * time.Millisecond)
	a.mustExec("SELECT 1") // no transaction was lost, so no error
}

func TestExpiredSessionIsSweptInBackground(t *testing.T) {
	e := newEnv(t, 1, libsqlhandler.WithSessionTimeout(50*time.Millisecond))
	a := e.client(true)
	a.mustExec("CREATE TABLE t (x)")
	a.mustExec("BEGIN", "INSERT INTO t VALUES (1)")

	// The session pins the only connection; the pool gets it back only once swept.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := e.db.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("connection never returned to the pool: %v", err)
	}
	if n != 0 {
		t.Fatalf("expired transaction leaked into the pool, count = %d", n)
	}
	if _, err := e.db.ExecContext(ctx, "BEGIN; ROLLBACK"); err != nil {
		t.Fatalf("pool connection still inside a transaction: %v", err)
	}
	if status, body := a.post(`{"statements": ["COMMIT"]}`); status != http.StatusConflict {
		t.Fatalf("client must learn its swept transaction was lost, got %d %s", status, body)
	}
}

// A cookie-keeping client looks one-shot until its second request; if that
// first request opened a transaction, evicting it would lose the transaction.
func TestFirstRequestTransactionIsNotEvicted(t *testing.T) {
	e := newEnv(t, 0, libsqlhandler.WithMaxSessions(1))
	a := e.client(true)
	a.mustExec("CREATE TABLE IF NOT EXISTS t (x)", "BEGIN", "INSERT INTO t VALUES (1)")

	if status, _ := e.client(false).post(`{"statements": ["SELECT 1"]}`); status != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503 rather than evicting a's transaction", status)
	}
	a.mustExec("INSERT INTO t VALUES (2)", "COMMIT")
	if got := a.scalar("SELECT count(*) FROM t"); got != 2.0 {
		t.Fatalf("count = %v", got)
	}
}

func TestConcurrentRequestsOnOneSession(t *testing.T) {
	e := newEnv(t, 0)
	c := e.client(true)
	c.mustExec("CREATE TABLE t (x)")

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() { c.mustExec(fmt.Sprintf("INSERT INTO t VALUES (%d)", i)) })
	}
	wg.Wait()
	if got := c.scalar("SELECT count(*) FROM t"); got != 20.0 {
		t.Fatalf("count = %v", got)
	}
}

func TestConcurrentSessionsWriting(t *testing.T) {
	e := newEnv(t, 0)
	e.client(false).mustExec("CREATE TABLE t (x)")

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			c := e.client(true)
			for j := range 5 {
				c.mustExec(fmt.Sprintf("INSERT INTO t VALUES (%d)", i*10+j))
			}
		})
	}
	wg.Wait()
	if got := e.client(false).scalar("SELECT count(*) FROM t"); got != 50.0 {
		t.Fatalf("count = %v", got)
	}
}
