package tests

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	libsqlhandler "github.com/sh-lucas/libsql-handler"
)

// Regressions found in review; each test failed before its fix.

// An expired session with an open transaction used to be reported once: a
// second request with the same old cookie got a fresh session and its
// writes were autocommitted outside the lost transaction.
func TestLostSessionRejectsEveryRequestWithItsCookie(t *testing.T) {
	e := newEnv(t, 0, libsqlhandler.WithSessionTimeout(50*time.Millisecond))
	a := e.client(true)
	a.mustExec("CREATE TABLE t (x)")
	a.mustExec("BEGIN", "INSERT INTO t VALUES (1)")
	u, _ := http.NewRequest(http.MethodPost, e.srv.URL, nil)
	cookie := a.http.Jar.Cookies(u.URL)[0]
	time.Sleep(200 * time.Millisecond)

	for i := range 2 {
		req, _ := http.NewRequest(http.MethodPost, e.srv.URL, strings.NewReader(`{"statements": ["INSERT INTO t VALUES (2)", "COMMIT"]}`))
		req.AddCookie(cookie)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("request %d with the lost cookie: got %d", i, resp.StatusCode)
		}
		if c := resp.Cookies(); len(c) != 1 || c[0].Name != "session" || c[0].MaxAge >= 0 {
			t.Fatalf("request %d: the 409 must clear the cookie, got %+v", i, c)
		}
	}
	if got := e.client(false).scalar("SELECT count(*) FROM t"); got != 0.0 {
		t.Fatalf("a request with the lost cookie ran, count = %v", got)
	}

	// A cookie-keeping client drops the cookie and starts over.
	status, _ := a.post(`{"statements": ["SELECT 1"]}`)
	if status != http.StatusConflict {
		t.Fatalf("got %d", status)
	}
	if got := a.scalar("SELECT 1"); got != 1.0 {
		t.Fatalf("fresh session: %v", got)
	}
}

// A request the server cannot decode is a 4xx, after which the client
// considers the stream gone; it used to keep its transaction open.
func TestHranaMalformedRequestClosesItsStream(t *testing.T) {
	e := newEnv(t, 1)
	s := e.stream()
	s.run(execSQL("CREATE TABLE t (x)"), execSQL("BEGIN"), execSQL("INSERT INTO t VALUES (1)"))

	status, body := postJSON(t, e.srv.URL, "/v3/pipeline", map[string]any{"baton": *s.baton, "requests": []any{map[string]any{"type": "nope"}}})
	if status != http.StatusBadRequest {
		t.Fatalf("got %d %s", status, body)
	}
	if got := e.stream().scalar("SELECT count(*) FROM t").String(); got != `integer:"0"` {
		t.Fatalf("transaction survived: %s", got)
	}
	if status, _ := s.raw(execSQL("COMMIT")); status != http.StatusBadRequest {
		t.Fatalf("baton must be invalid, got %d", status)
	}
}

const endless = "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c) SELECT count(*) FROM c"

// A client that gives up mid-request never sees the next baton; its stream
// used to stay open with the transaction holding the write lock.
func TestHranaCanceledRequestReleasesItsStream(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	s.run(execSQL("CREATE TABLE t (x)"), execSQL("BEGIN"), execSQL("INSERT INTO t VALUES (1)"))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.srv.URL+"/v3/pipeline",
		strings.NewReader(`{"baton": "`+*s.baton+`", "requests": [{"type": "execute", "stmt": {"sql": "`+endless+`"}}]}`))
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
		t.Fatal("the endless query returned")
	}

	// The write lock must come back promptly, with the transaction rolled back.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := e.db.Exec("INSERT INTO t VALUES (2)")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("write lock still held: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var n int
	if err := e.db.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil || n != 1 {
		t.Fatalf("count = %d, err = %v (want only the outside row)", n, err)
	}
}

// The cursor header used to sit in a buffer until a slow step finished.
func TestHranaCursorSendsHeaderBeforeSlowStep(t *testing.T) {
	e := newEnv(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.srv.URL+"/v3/cursor",
		strings.NewReader(`{"baton": null, "batch": {"steps": [{"stmt": {"sql": "SELECT 1"}}, {"stmt": {"sql": "`+endless+`"}}]}}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	lines := make(chan string, 8)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	for _, want := range []string{`"baton":`, `"step_begin"`, `"row"`, `"step_end"`} {
		select {
		case line := <-lines:
			if !strings.Contains(line, want) {
				t.Fatalf("got %s, want %s", line, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no %s line while a later step runs", want)
		}
	}
}

// Close used to wait for requests still uploading their body.
func TestCloseDoesNotWaitForSlowRequestBodies(t *testing.T) {
	e := newEnv(t, 0)
	for _, path := range []string{"/", "/v3/pipeline", "/v3/cursor"} {
		pr, pw := io.Pipe()
		defer pw.Close()
		go func() {
			resp, err := http.Post(e.srv.URL+path, "application/json", pr)
			if err == nil {
				resp.Body.Close()
			}
		}()
		if _, err := pw.Write([]byte(`{"baton": null, `)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond) // let the handlers start reading

	closed := make(chan struct{})
	go func() { e.h.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close is waiting on unfinished request bodies")
	}
}

// Probing autocommit runs BEGIN/ROLLBACK, and ending a transaction resets
// PRAGMA defer_foreign_keys; the probe must not change it.
func TestAutocommitProbeKeepsDeferForeignKeys(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	res := s.run(
		map[string]any{"type": "sequence", "sql": "PRAGMA defer_foreign_keys = ON"},
		map[string]any{"type": "get_autocommit"},
		batchReq(step(stmt("SELECT 1"), map[string]any{"type": "is_autocommit"})),
		execSQL("PRAGMA defer_foreign_keys"),
	)
	if got := row(execResult(t, res[3]).Rows[0]); got != `integer:"1"` {
		t.Fatalf("defer_foreign_keys = %s", got)
	}
}

// The origin check used to run after reading the body: a hostile page could
// make the server buffer an arbitrary upload before getting its 403. Raw TCP,
// since http.Client waits for its own upload.
func TestCrossOriginRejectedBeforeReadingBody(t *testing.T) {
	e := newEnv(t, 0)
	for _, path := range []string{"/", "/v3/pipeline", "/v3/cursor"} {
		conn, err := net.Dial("tcp", strings.TrimPrefix(e.srv.URL, "http://"))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: x\r\nSec-Fetch-Site: cross-site\r\nContent-Type: application/json\r\nContent-Length: 1000000\r\n\r\n{\"baton\": null, ", path)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		status, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil || !strings.Contains(status, "403") {
			t.Errorf("%s: got %q, %v (the server is waiting on the body?)", path, status, err)
		}
	}
}

// A CTE named like a verb made a SELECT report the previous statement's
// changes() and last_insert_rowid as its own.
func TestHranaCTENamedLikeVerb(t *testing.T) {
	e := newEnv(t, 0)
	s := e.stream()
	s.run(execSQL("CREATE TABLE t (x)"), execSQL("INSERT INTO t VALUES (1), (2)"))
	r := s.exec("WITH replace AS (SELECT 1) SELECT * FROM replace")
	if r.AffectedRowCount != 0 || r.LastInsertRowid != nil {
		t.Fatalf("SELECT reported affected=%d rowid=%v", r.AffectedRowCount, r.LastInsertRowid)
	}
}
