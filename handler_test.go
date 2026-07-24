package libsqlhandler

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestVersionEndpoint(t *testing.T) {
	// Initialize a temporary in-memory database handler
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	h := New(db)
	defer h.Close()

	server := httptest.NewServer(h)
	defer server.Close()

	resp, err := http.Get(server.URL + "/version")
	if err != nil {
		t.Fatalf("failed GET /version: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	versionStr := body.String()
	if !strings.HasPrefix(versionStr, "sqld 0.20.0") {
		t.Errorf("unexpected version response: %q", versionStr)
	}
}

func TestExecuteSimpleAndParameterized(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	h := New(db)
	defer h.Close()

	server := httptest.NewServer(h)
	defer server.Close()

	// 1. Create table and insert a row
	reqPayload := Request{
		Statements: []Statement{
			{Query: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"},
			{Query: "INSERT INTO users (name) VALUES ('Lucas')"},
			{
				Query:  "SELECT * FROM users WHERE name = ?",
				Params: []interface{}{"Lucas"},
			},
		},
	}

	reqBytes, _ := json.Marshal(reqPayload)
	resp, err := http.Post(server.URL, "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		t.Fatalf("failed POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var results []StatementResponse
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		t.Fatalf("failed to decode responses: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(results))
	}

	// Check Table creation response
	if results[0].Error != "" {
		t.Errorf("error creating table: %s", results[0].Error)
	}

	// Check Insert response (should have rows_written = 1)
	if results[1].Error != "" {
		t.Errorf("error inserting: %s", results[1].Error)
	}
	if results[1].Results.RowsWritten != 1 {
		t.Errorf("expected rows_written = 1, got %d", results[1].Results.RowsWritten)
	}

	// Check Select response
	selRes := results[2]
	if selRes.Error != "" {
		t.Fatalf("error selecting: %s", selRes.Error)
	}
	if len(selRes.Results.Columns) != 2 || selRes.Results.Columns[1] != "name" {
		t.Errorf("unexpected columns: %v", selRes.Results.Columns)
	}
	if len(selRes.Results.Rows) != 1 || selRes.Results.Rows[0][1] != "Lucas" {
		t.Errorf("unexpected rows: %v", selRes.Results.Rows)
	}
}

func TestSessionStateTransactions(t *testing.T) {
	// Dynamically create a unique temp file for the SQLite database
	tempFile, err := os.CreateTemp("data", "libsql_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tempFileName := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempFileName) // clean up after test completes

	db, err := sql.Open("sqlite", tempFileName)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	h := New(db)
	defer h.Close()

	server := httptest.NewServer(h)
	defer server.Close()

	// Create a client with a CookieJar to simulate DBeaver's CookieManager
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}

	// 1. Initial setup (Create table)
	setupPayload, _ := json.Marshal(Request{
		Statements: []Statement{
			{Query: "DROP TABLE IF EXISTS accounts"},
			{Query: "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL)"},
		},
	})
	client.Post(server.URL, "application/json", bytes.NewReader(setupPayload))

	// 2. Start a transaction and insert a row in Session A
	txPayload, _ := json.Marshal(Request{
		Statements: []Statement{
			{Query: "BEGIN TRANSACTION"},
			{Query: "INSERT INTO accounts (id, balance) VALUES (1, 100.0)"},
		},
	})
	resp, err := client.Post(server.URL, "application/json", bytes.NewReader(txPayload))
	if err != nil {
		t.Fatalf("failed in tx post: %v", err)
	}
	resp.Body.Close()

	// Verify session cookie was set
	u, _ := url.Parse(server.URL)
	cookies := jar.Cookies(u)
	if len(cookies) == 0 || cookies[0].Name != "session" {
		t.Fatalf("expected session cookie to be set, got none")
	}

	// 3. Query the balance *inside* the same session connection. It should see the uncommitted row!
	queryPayload1, _ := json.Marshal(Request{
		Statements: []Statement{
			{Query: "SELECT balance FROM accounts WHERE id = 1"},
		},
	})
	resp1, err := client.Post(server.URL, "application/json", bytes.NewReader(queryPayload1))
	if err != nil {
		t.Fatalf("failed in query inside tx: %v", err)
	}
	defer resp1.Body.Close()

	var res1 []StatementResponse
	json.NewDecoder(resp1.Body).Decode(&res1)
	if len(res1) == 0 || res1[0].Error != "" {
		t.Fatalf("query failed: %v", res1[0].Error)
	}
	if len(res1[0].Results.Rows) == 0 || res1[0].Results.Rows[0][0].(float64) != 100.0 {
		t.Errorf("expected balance 100.0 inside transaction, got: %v", res1[0].Results.Rows)
	}

	// 4. Query the balance in a *different* stateless session (no cookies). It should NOT see the row!
	clientNoCookie := &http.Client{} // No jar
	resp2, err := clientNoCookie.Post(server.URL, "application/json", bytes.NewReader(queryPayload1))
	if err != nil {
		t.Fatalf("failed in query outside tx: %v", err)
	}
	defer resp2.Body.Close()

	var res2 []StatementResponse
	json.NewDecoder(resp2.Body).Decode(&res2)
	if len(res2) > 0 && res2[0].Error == "" {
		if len(res2[0].Results.Rows) > 0 {
			t.Errorf("uncommitted transaction leak! stateless client saw row: %v", res2[0].Results.Rows)
		}
	}

	// 5. Commit the transaction in Session A
	commitPayload, _ := json.Marshal(Request{
		Statements: []Statement{
			{Query: "COMMIT"},
		},
	})
	resp3, _ := client.Post(server.URL, "application/json", bytes.NewReader(commitPayload))
	resp3.Body.Close()

	// 6. Now the stateless client should be able to see the committed row!
	resp4, _ := clientNoCookie.Post(server.URL, "application/json", bytes.NewReader(queryPayload1))
	defer resp4.Body.Close()
	var res4 []StatementResponse
	json.NewDecoder(resp4.Body).Decode(&res4)
	if len(res4) == 0 || len(res4[0].Results.Rows) == 0 || res4[0].Results.Rows[0][0].(float64) != 100.0 {
		t.Errorf("expected committed balance 100.0, got: %v", res4[0].Results.Rows)
	}
}

func TestBatchErrorHandlingStopOnFailure(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	h := New(db)
	defer h.Close()

	server := httptest.NewServer(h)
	defer server.Close()

	reqPayload := Request{
		Statements: []Statement{
			{Query: "SELECT 1 AS first"},
			{Query: "SELECT * FROM non_existent_table_xyz"},
			{Query: "SELECT 2 AS second"},
		},
	}

	reqBytes, _ := json.Marshal(reqPayload)
	resp, err := http.Post(server.URL, "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		t.Fatalf("failed POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var rawResponses []json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&rawResponses); err != nil {
		t.Fatalf("failed to decode raw responses: %v", err)
	}

	if len(rawResponses) != 3 {
		t.Fatalf("expected 3 items in response array, got %d", len(rawResponses))
	}

	// 1st statement should succeed
	var stmt1 StatementResponse
	if err := json.Unmarshal(rawResponses[0], &stmt1); err != nil {
		t.Fatalf("failed unmarshaling stmt 1: %v", err)
	}
	if stmt1.Error != "" || len(stmt1.Results.Rows) != 1 {
		t.Errorf("stmt 1 unexpected result: %+v", stmt1)
	}

	// 2nd statement should fail
	var stmt2 StatementResponse
	if err := json.Unmarshal(rawResponses[1], &stmt2); err != nil {
		t.Fatalf("failed unmarshaling stmt 2: %v", err)
	}
	if stmt2.Error == "" || !strings.Contains(stmt2.Error, "no such table") {
		t.Errorf("expected error in stmt 2, got: %q", stmt2.Error)
	}

	// 3rd statement should be null (skipped after error in batch)
	if string(rawResponses[2]) != "null" {
		t.Errorf("expected 3rd statement response to be null, got: %s", string(rawResponses[2]))
	}
}

func TestReturningClauseQueries(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	h := New(db)
	defer h.Close()

	server := httptest.NewServer(h)
	defer server.Close()

	reqPayload := Request{
		Statements: []Statement{
			{Query: "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"},
			{Query: "INSERT INTO items (name) VALUES ('widget') RETURNING id, name"},
		},
	}

	reqBytes, _ := json.Marshal(reqPayload)
	resp, err := http.Post(server.URL, "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		t.Fatalf("failed POST: %v", err)
	}
	defer resp.Body.Close()

	var results []StatementResponse
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		t.Fatalf("failed to decode responses: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(results))
	}

	retRes := results[1]
	if retRes.Error != "" {
		t.Fatalf("error in RETURNING query: %s", retRes.Error)
	}
	if len(retRes.Results.Columns) != 2 || retRes.Results.Columns[0] != "id" || retRes.Results.Columns[1] != "name" {
		t.Errorf("unexpected RETURNING columns: %v", retRes.Results.Columns)
	}
	if len(retRes.Results.Rows) != 1 || retRes.Results.Rows[0][1] != "widget" {
		t.Errorf("unexpected RETURNING rows: %v", retRes.Results.Rows)
	}
}

func TestNamedParametersVariants(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	h := New(db)
	defer h.Close()

	server := httptest.NewServer(h)
	defer server.Close()

	reqPayload := Request{
		Statements: []Statement{
			{
				Query: "SELECT :a AS first, @b AS second, $c AS third",
				NamedParams: map[string]interface{}{
					":a": 10,
					"b":  20,
					"$c": 30,
				},
			},
		},
	}

	reqBytes, _ := json.Marshal(reqPayload)
	resp, err := http.Post(server.URL, "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		t.Fatalf("failed POST: %v", err)
	}
	defer resp.Body.Close()

	var results []StatementResponse
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(results) != 1 || results[0].Error != "" {
		t.Fatalf("named params query failed: %v", results[0].Error)
	}

	rows := results[0].Results.Rows
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if rows[0][0].(float64) != 10 || rows[0][1].(float64) != 20 || rows[0][2].(float64) != 30 {
		t.Errorf("unexpected named params values: %v", rows[0])
	}
}

func TestCORSAndOptions(t *testing.T) {
	db, _ := sql.Open("sqlite", ":memory:")
	defer db.Close()
	h := New(db)
	defer h.Close()
	server := httptest.NewServer(h)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodOptions, server.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed OPTIONS request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for OPTIONS, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("expected Access-Control-Allow-Origin: *, got %q", resp.Header.Get("Access-Control-Allow-Origin"))
	}
}

func TestCompareWithRealSqld(t *testing.T) {
	sqldURL := "http://localhost:8085"
	// Check if real sqld is available on localhost:8085
	respCheck, err := http.Get(sqldURL + "/version")
	if err != nil || respCheck.StatusCode != http.StatusOK {
		t.Skip("real sqld instance not running on localhost:8085, skipping live comparative test")
		return
	}
	respCheck.Body.Close()

	// Setup local libsql-handler server
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()
	h := New(db)
	defer h.Close()
	handlerServer := httptest.NewServer(h)
	defer handlerServer.Close()

	// Test case 1: Query output format comparison
	queryJSON := `{"statements": ["SELECT 42 AS num, 'hello' AS msg, NULL as empty"]}`
	
	respHandler, err := http.Post(handlerServer.URL, "application/json", strings.NewReader(queryJSON))
	if err != nil {
		t.Fatalf("handler POST failed: %v", err)
	}
	defer respHandler.Body.Close()

	respSqld, err := http.Post(sqldURL, "application/json", strings.NewReader(queryJSON))
	if err != nil {
		t.Fatalf("sqld POST failed: %v", err)
	}
	defer respSqld.Body.Close()

	var handlerRes []StatementResponse
	var sqldRes []StatementResponse
	json.NewDecoder(respHandler.Body).Decode(&handlerRes)
	json.NewDecoder(respSqld.Body).Decode(&sqldRes)

	if len(handlerRes) != len(sqldRes) {
		t.Errorf("response length mismatch: handler=%d, sqld=%d", len(handlerRes), len(sqldRes))
	} else {
		// Compare Columns
		if len(handlerRes[0].Results.Columns) != len(sqldRes[0].Results.Columns) {
			t.Errorf("columns length mismatch: handler=%v, sqld=%v", handlerRes[0].Results.Columns, sqldRes[0].Results.Columns)
		}
		// Compare Rows content
		if len(handlerRes[0].Results.Rows) != len(sqldRes[0].Results.Rows) {
			t.Errorf("rows length mismatch: handler=%v, sqld=%v", handlerRes[0].Results.Rows, sqldRes[0].Results.Rows)
		}
	}

	// Test case 2: Batch failure comparison
	batchErrJSON := `{"statements": ["SELECT 1", "SELECT * FROM missing_table_123", "SELECT 2"]}`
	
	respHandlerErr, _ := http.Post(handlerServer.URL, "application/json", strings.NewReader(batchErrJSON))
	defer respHandlerErr.Body.Close()
	respSqldErr, _ := http.Post(sqldURL, "application/json", strings.NewReader(batchErrJSON))
	defer respSqldErr.Body.Close()

	var rawHandlerErr []json.RawMessage
	var rawSqldErr []json.RawMessage
	json.NewDecoder(respHandlerErr.Body).Decode(&rawHandlerErr)
	json.NewDecoder(respSqldErr.Body).Decode(&rawSqldErr)

	if string(rawHandlerErr[2]) != string(rawSqldErr[2]) {
		t.Errorf("batch failure handling mismatch on 3rd item: handler=%s, sqld=%s", string(rawHandlerErr[2]), string(rawSqldErr[2]))
	}
}

