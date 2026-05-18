package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
)

func main() {
	port := flag.Int("port", 8080, "Port to run the HTTP server on")
	dbPath := flag.String("db", "local.db", "SQLite database file path")
	flag.Parse()

	log.Printf("Initializing LibSQL HTTP handler using database file: %s...", *dbPath)

	// Create and initialize the LibSQLHandler using the modernc.org/sqlite (pure Go) driver
	h, err := NewLibSQLHandler("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer h.Close()

	// Print beautiful connection details for DBeaver
	fmt.Println(`
================================================================================
  LibSQL Go Handler started successfully!
================================================================================

  How to connect to this server from DBeaver:

  1. Open DBeaver and go to "Database" -> "New Database Connection".
  2. Search and select the "LibSQL" driver (included by default).
  3. Configure the connection settings:
     - URL: http://localhost:` + fmt.Sprintf("%d", *port) + `
     - Host: localhost
     - Port: ` + fmt.Sprintf("%d", *port) + `
     - Username: (leave empty)
     - Password: (leave empty, or any token)
  4. Test the connection! It will connect to your local SQLite file: %s

================================================================================`)

	// Start the HTTP server with our handler
	serverAddr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting HTTP server on %s...", serverAddr)
	if err := http.ListenAndServe(serverAddr, h); err != nil {
		if err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}
}

// Ensure local.db is closed if the process is terminated
func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
