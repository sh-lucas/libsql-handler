package tests

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Close used to panic on a second call, and requests arriving after it
// panicked sending on the closed flush channel.
func TestCloseIsIdempotentAndRejectsLaterRequests(t *testing.T) {
	e := newEnv(t, 0)
	e.client(false).mustExec("SELECT 1")

	if err := e.h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.h.Close(); err != nil {
		t.Fatal(err)
	}
	status, body := e.client(false).post(`{"statements": ["SELECT 1"]}`)
	if status != http.StatusServiceUnavailable || !strings.Contains(body, "closed") {
		t.Fatalf("got %d %s", status, body)
	}
}

func TestCloseRollsBackOpenTransactions(t *testing.T) {
	e := newEnv(t, 0)
	a := e.client(true)
	a.mustExec("CREATE TABLE t (x)")
	a.mustExec("BEGIN", "INSERT INTO t VALUES (1)")
	e.h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := e.db.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil || n != 0 {
		t.Fatalf("count = %d, err = %v", n, err)
	}
	// Fails with SQLITE_BUSY if the closed session still holds the write lock.
	if _, err := e.db.ExecContext(ctx, "INSERT INTO t VALUES (2)"); err != nil {
		t.Fatalf("write lock not released: %v", err)
	}
}

func TestCloseInterruptsRunningQueries(t *testing.T) {
	e := newEnv(t, 0)
	c := e.client(true)
	c.mustExec("SELECT 1")

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.post(`{"statements": ["WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n) SELECT count(*) FROM n"]}`)
	}()
	time.Sleep(50 * time.Millisecond) // either way Close must return promptly

	closed := make(chan struct{})
	go func() { e.h.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked on an endless query")
	}
	<-done
}

// A request waiting for a pool connection is not a session yet; Close used
// to return without waiting for it, while it could still grab a connection.
func TestCloseCancelsPendingConnectionWaits(t *testing.T) {
	e := newEnv(t, 1)
	lease, err := e.db.Conn(context.Background()) // hold the only connection
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()

	status := make(chan int, 1)
	go func() {
		s, _ := e.client(false).post(`{"statements": ["SELECT 1"]}`)
		status <- s
	}()
	for e.db.Stats().WaitCount == 0 {
		time.Sleep(time.Millisecond)
	}

	e.h.Close()
	select {
	case s := <-status:
		if s != http.StatusServiceUnavailable {
			t.Fatalf("got %d", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request still waiting for a connection after Close returned")
	}
}
