// Package clients checks the handler with the official Go client
// (libsql-client-go, Hrana over HTTP). Run with `just test-clients`;
// LIBSQL_URL points at a running handler.
package clients

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
)

func TestGoClient(t *testing.T) {
	url := os.Getenv("LIBSQL_URL")
	if url == "" {
		t.Skip("LIBSQL_URL not set")
	}
	db, err := sql.Open("libsql", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table := fmt.Sprintf("go_%d", time.Now().UnixNano())

	var n int64
	var f float64
	var s string
	var b []byte
	if err := db.QueryRow("SELECT ?, ?, ?, ?", int64(9007199254740993), 1.5, "héllo", []byte{0, 255}).Scan(&n, &f, &s, &b); err != nil {
		t.Fatal(err)
	}
	if n != 9007199254740993 || f != 1.5 || s != "héllo" || string(b) != "\x00\xff" {
		t.Fatalf("got %v %v %q %v", n, f, s, b)
	}

	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, v TEXT)", table)); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE " + table)
	res, err := db.Exec(fmt.Sprintf("INSERT INTO %s (v) VALUES (?), (?)", table), "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	if affected, _ := res.RowsAffected(); affected != 2 {
		t.Fatalf("RowsAffected = %d", affected)
	}
	if id, _ := res.LastInsertId(); id != 2 {
		t.Fatalf("LastInsertId = %d", id)
	}

	// Rolled back and committed transactions.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (v) VALUES ('c')", table)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (v) VALUES ('d')", table)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 3 {
		t.Fatalf("count = %d, err = %v", count, err)
	}

	if _, err := db.Exec("SELECT * FROM missing_table"); err == nil {
		t.Fatal("want error")
	}
}
