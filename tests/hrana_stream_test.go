package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	libsqlhandler "github.com/sh-lucas/libsql-handler"
)

func TestHranaStreamLifecycle(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	s.run(execSQL("CREATE TABLE t (x)"))
	if s.baton == nil {
		t.Fatal("an open stream must return a baton")
	}
	old := *s.baton
	s.run(execSQL("INSERT INTO t VALUES (1)"))

	// Each baton is single-use: replaying an old one is rejected.
	stale := &hstream{t: t, url: e.srv.URL, path: "/v3/pipeline", baton: &old}
	if status, body := stale.raw(execSQL("SELECT 1")); status != http.StatusBadRequest {
		t.Fatalf("stale baton: got %d %s", status, body)
	}

	res := s.run(execSQL("SELECT count(*) FROM t"), closeReq)
	if s.baton != nil {
		t.Fatalf("closed stream must return a null baton, got %q", *s.baton)
	}
	if got := row(execResult(t, res[0]).Rows[0]); got != `integer:"1"` {
		t.Fatalf("got %s", got)
	}
}

func TestHranaInteractiveTransaction(t *testing.T) {
	e := newEnv(t, 0)
	setup := e.stream()
	setup.run(execSQL("CREATE TABLE t (x)"), closeReq)

	a, other := e.stream(), e.stream()
	a.run(execSQL("BEGIN"), execSQL("INSERT INTO t VALUES (1)"))
	if r := ok(t, a.run(map[string]any{"type": "get_autocommit"})[0]); *r.IsAutocommit {
		t.Fatal("stream inside BEGIN reports autocommit")
	}
	if got := other.scalar("SELECT count(*) FROM t").String(); got != `integer:"0"` {
		t.Fatalf("uncommitted row leaked to another stream: %s", got)
	}
	if got := a.scalar("SELECT count(*) FROM t").String(); got != `integer:"1"` {
		t.Fatalf("own stream must see its row: %s", got)
	}
	a.run(execSQL("COMMIT"), closeReq)
	if got := other.scalar("SELECT count(*) FROM t").String(); got != `integer:"1"` {
		t.Fatalf("committed row not visible: %s", got)
	}
}

func TestHranaCloseRollsBack(t *testing.T) {
	e := newEnv(t, 1) // one connection: a leaked transaction would be visible
	s := e.stream()
	s.run(execSQL("CREATE TABLE t (x)"), execSQL("BEGIN"), execSQL("INSERT INTO t VALUES (1)"), closeReq)

	s2 := e.stream()
	if got := s2.scalar("SELECT count(*) FROM t").String(); got != `integer:"0"` {
		t.Fatalf("closed stream's transaction was not rolled back: %s", got)
	}
	s2.run(execSQL("BEGIN"), execSQL("ROLLBACK"), closeReq)
}

// A protocol error invalidates the stream and rolls back its transaction.
func TestHranaProtocolErrorClosesStream(t *testing.T) {
	e := newEnv(t, 1)
	s := e.stream()
	s.run(execSQL("CREATE TABLE t (x)"), execSQL("BEGIN"), execSQL("INSERT INTO t VALUES (1)"))
	if status, _ := s.raw(execute(map[string]any{"sql_id": 42})); status != http.StatusBadRequest {
		t.Fatalf("got %d", status)
	}
	if status, _ := s.raw(execSQL("COMMIT")); status != http.StatusBadRequest {
		t.Fatalf("stream must be gone after a protocol error, got %d", status)
	}
	if got := e.stream().scalar("SELECT count(*) FROM t").String(); got != `integer:"0"` {
		t.Fatalf("transaction survived: %s", got)
	}
}

func TestHranaExpiredStreamIsRolledBack(t *testing.T) {
	e := newEnv(t, 1, libsqlhandler.WithSessionTimeout(50*time.Millisecond))
	s := e.stream()
	s.run(execSQL("CREATE TABLE t (x)"), execSQL("BEGIN"), execSQL("INSERT INTO t VALUES (1)"))
	time.Sleep(200 * time.Millisecond)

	status, body := s.raw(execSQL("COMMIT"))
	if status != http.StatusBadRequest || !strings.Contains(body, "rolled back") {
		t.Fatalf("got %d %s", status, body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := e.db.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil || n != 0 {
		t.Fatalf("count = %d, err = %v", n, err)
	}
}

// One-shot streams (opened and closed in the same request, as the TS client
// does for every execute) never pile up against the session limit.
func TestHranaOneShotStreamsDoNotLeak(t *testing.T) {
	e := newEnv(t, 2, libsqlhandler.WithMaxSessions(1))
	for range 50 {
		s := e.stream()
		s.run(execSQL("SELECT 1"), closeReq)
	}
	// An abandoned stream holds its slot until it expires: the limit applies.
	held := e.stream()
	held.run(execSQL("SELECT 1"))
	status, body := e.stream().raw(execSQL("SELECT 1"))
	var msg hError
	if status != http.StatusServiceUnavailable || json.Unmarshal([]byte(body), &msg) != nil || msg.Message == "" {
		t.Fatalf("got %d %s", status, body)
	}
	held.run(closeReq)
	e.stream().run(execSQL("SELECT 1"), closeReq)
}

// Requests racing on the same baton: exactly one wins, the other is rejected
// instead of running on a stream state it did not see.
func TestHranaConcurrentBatonUse(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	s.run(execSQL("SELECT 1"))
	baton := *s.baton

	var wg sync.WaitGroup
	statuses := make([]int, 8)
	for i := range statuses {
		wg.Go(func() {
			c := &hstream{t: t, url: e.srv.URL, path: "/v3/pipeline", baton: &baton}
			statuses[i], _ = c.raw(execSQL("SELECT 1"))
		})
	}
	wg.Wait()
	okCount := 0
	for _, st := range statuses {
		switch st {
		case http.StatusOK:
			okCount++
		case http.StatusBadRequest:
		default:
			t.Fatalf("unexpected status %d", st)
		}
	}
	if okCount != 1 {
		t.Fatalf("%d requests succeeded with one baton: %v", okCount, statuses)
	}
}

// v1 (DBeaver) and Hrana share the database and its locking.
func TestHranaAndV1Coexist(t *testing.T) {
	e := newEnv(t, 0)
	v1 := e.client(true)
	v1.mustExec("CREATE TABLE t (x)", "INSERT INTO t VALUES (1)")
	s := e.stream()
	s.run(execSQL("INSERT INTO t VALUES (2)"), closeReq)
	if got := v1.scalar("SELECT count(*) FROM t"); got != 2.0 {
		t.Fatalf("count = %v", got)
	}
}

func TestHranaCloseInterruptsStreams(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	s.run(execSQL("CREATE TABLE t (x)"), execSQL("BEGIN"), execSQL("INSERT INTO t VALUES (1)"))
	e.h.Close()
	status, body := s.raw(execSQL("COMMIT"))
	if status != http.StatusServiceUnavailable {
		t.Fatalf("got %d %s", status, body)
	}
	var n int
	if err := e.db.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil || n != 0 {
		t.Fatalf("count = %d, err = %v", n, err)
	}
}
