// Command server serves a local SQLite file to libSQL clients and DBeaver.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	libsqlhandler "github.com/sh-lucas/libsql-handler"

	_ "modernc.org/sqlite"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address; anyone who can reach it gets full SQL access")
	dbPath := flag.String("db", "data/local.db", "SQLite database file")
	verbose := flag.Bool("v", false, "log every executed statement")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if err := run(*addr, *dbPath, log); err != nil {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func run(addr, dbPath string, log *slog.Logger) error {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(100)

	h := libsqlhandler.New(db, libsqlhandler.WithLogger(log))
	defer h.Close()

	var handler http.Handler = h
	if isLoopback(addr) {
		handler = loopbackHostOnly(h)
	}
	srv := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		grace, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(grace)
		// Past the grace period: drop connections first, so a handler blocked
		// writing to a client that stopped reading returns, then interrupt
		// remaining queries and roll back sessions.
		_ = srv.Close()
		_ = h.Close()
	}()

	fmt.Fprintf(os.Stderr, "Serving %s on http://%s (libSQL clients: url http://%s, no token; DBeaver: LibSQL connection, same URL)\n", dbPath, addr, addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-shutdownDone
	return nil
}

// isLoopback reports whether a "host" or "host:port" names this machine only.
func isLoopback(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// loopbackHostOnly blocks DNS rebinding: a web page on evil.example whose
// name resolves to 127.0.0.1 is same-origin to the browser, but still sends
// Host: evil.example.
func loopbackHostOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isLoopback(r.Host) {
			http.Error(w, "Forbidden host", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
