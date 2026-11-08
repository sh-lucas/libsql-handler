# libsql-handler

Serves a SQLite `*sql.DB` over the libSQL (sqld) v1 HTTP protocol, so DBeaver's
built-in **LibSQL** driver can browse and edit a local SQLite file.
You don't need sqld to have an exposed sqlite by libsql.

```go
db, _ := sql.Open("sqlite", "data/app.db") // any database/sql SQLite driver
h := libsqlhandler.New(db)                  // options: WithSessionTimeout, WithMaxSessions, WithLogger
defer db.Close()
defer h.Close()
http.ListenAndServe("127.0.0.1:8080", h)
```

Or run the bundled server: `just run -db data/app.db -v`, then in DBeaver create a
LibSQL connection with URL `http://127.0.0.1:8080`.

## Behavior

- **Sessions.** Each client gets a `session` cookie pinned to its own connection,
  so transactions can span requests (sqld's v1 protocol cannot). Idle sessions
  are rolled back and dropped after the timeout (default 10 min). An idle
  session without a transaction is replaced silently. If the session had a
  transaction open, the client's next request fails with `409` and runs
  nothing, so the rest of the transaction is never autocommitted on a fresh
  session. After a process restart, all sessions are simply new.
- **Capacity.** Each session holds one pool connection. The session limit is
  `min(WithMaxSessions, MaxOpenConns-1)`, leaving one connection free for WAL
  checkpoints. At the limit, the oldest session of a client that never sent its
  cookie back (curl, scripts) and has no open transaction is evicted. If there
  is none, the request gets `503` immediately.
- **Locking.** An open transaction in one session blocks writers in the others,
  and they fail with `SQLITE_BUSY` after 5 s. That comes from SQLite itself. Keep
  transactions short.
- **No auth.** Anyone who can reach the handler has full SQL access. Bind to
  localhost or put your own auth middleware in front. Cross-origin browser
  requests are rejected (no CORS, plus `http.CrossOriginProtection`). The
  bundled server, when bound to loopback, also rejects non-loopback `Host`
  headers to block DNS rebinding. If you embed the handler, add that check
  yourself.
- **Memory.** Each response is built in memory, like sqld v1, which has no
  paging. A huge `SELECT` costs that much RAM in your process.
- **Values.** Integers stay exact (int64). BLOBs travel as `{"base64": "..."}`
  in both results and params. Inf/NaN become `null`. `rows_read` is the number
  of rows returned, not the number SQLite scanned.
- **Close** interrupts running queries, rolls back every session and waits for
  in-flight requests.
- **Drivers.** Only `database/sql` is used. The test suite passes with
  modernc.org/sqlite, ncruces/go-sqlite3 and mattn/go-sqlite3. The driver chooses
  how values come back: DATE/DATETIME/TIMESTAMP columns parsed into `time.Time`
  are returned as `YYYY-MM-DD HH:MM:SS[.fff]`, which is exact for SQLite's own
  format but normalizes other stored formats.

## Development

`just test` runs the suite with `-race`, `just check` adds vet and gofmt, and
`just test-sqld` compares responses with a real sqld on `:8085`.
