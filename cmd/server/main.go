package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	libsqlhandler "github.com/sh-lucas/libsql-handler"

	_ "modernc.org/sqlite"
)

func main() {
	port := flag.Int("port", 8080, "Port to run the HTTP server on")
	dbPath := flag.String("db", "data/local.db", "SQLite database file path")
	flag.Parse()

	log.Printf("Opening SQLite database file using pure Go driver: %s...", *dbPath)

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	// Configure standard connection pool settings
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)

	// Create and initialize the LibSQLHandler using dependency injection
	h := libsqlhandler.New(db)
	defer h.Close()
	defer db.Close()

	// Print beautiful connection details for DBeaver
	fmt.Printf(`
================================================================================
  LibSQL Go Handler started successfully!
================================================================================

  How to connect to this server from DBeaver:

  1. Open DBeaver and go to "Database" -> "New Database Connection".
  2. Search and select the "LibSQL" driver (included by default).
  3. Configure the connection settings:
     - URL: http://localhost:%d
     - Host: localhost
     - Port: %d
     - Username: (leave empty)
     - Password: (leave empty, or any token)
  4. Test the connection! It will connect to your local SQLite file: %s

================================================================================
`, *port, *port, *dbPath)

	// Start the HTTP server with our handler
	serverAddr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting HTTP server on %s...", serverAddr)
	if err := http.ListenAndServe(serverAddr, h); err != nil {
		if err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
