# libsql-handler

Serves a SQLite `*sql.DB` over the libSQL (sqld) HTTP protocols, so libSQL
clients and tools can use a local SQLite file without running sqld:

- **Hrana over HTTP** (`/v2/pipeline`, `/v3/pipeline`, `/v3/cursor`, JSON), spoken
  by the official clients (`@libsql/client`, `libsql-client-go`, the Rust `libsql`
  crate) and the tools built on them.
- **The legacy v1 API** (`POST /`), the only one DBeaver's built-in **LibSQL**
  JDBC driver speaks.

```go
db, _ := sql.Open("sqlite", "data/app.db") // any database/sql SQLite driver
h := libsqlhandler.New(db)                  // options: WithSessionTimeout, WithMaxSessions, WithLogger
defer db.Close()
defer h.Close()
http.ListenAndServe("127.0.0.1:8080", h)
```

Or run the bundled server: `just run -db data/app.db -v`, then point a client at
`http://127.0.0.1:8080` (no auth token), e.g. `createClient({ url: "http://127.0.0.1:8080" })`,
or create a LibSQL connection in DBeaver with that URL.

## Behavior

- **Hrana streams.** Each stream is pinned to its own connection and addressed
  by a baton that changes with every response; a stale, replayed or forged
  baton gets `400` and runs nothing. A stream ends on `close`, on a protocol
  error (`400`), or after the idle timeout (default 10 min); its open
  transaction is rolled back. Each stream gets a fresh connection, so
  connection state (`PRAGMA foreign_keys`, temp tables) never leaks between
  streams or into your own use of the pool.
- **v1 sessions.** Each v1 client gets a `session` cookie pinned to its own
  connection, so transactions can span requests (sqld's v1 protocol cannot).
  Idle sessions are rolled back and dropped after the timeout. An idle session
  without a transaction is replaced silently. If the session had a transaction
  open, the client's next request fails with `409` and runs nothing, so the
  rest of the transaction is never autocommitted on a fresh session. After a
  process restart, all sessions are simply new.
- **Capacity.** Each session or stream holds one pool connection. The session limit is
  `min(WithMaxSessions, MaxOpenConns-1)`, leaving one connection free for WAL
  checkpoints. At the limit, the oldest session of a client that never sent its
  cookie back (curl, scripts) and has no open transaction is evicted. If there
  is none, the request gets `503` immediately. Hrana streams are never evicted:
  clients close them, and abandoned ones wait for the timeout.
- **Locking.** An open transaction in one session blocks writers in the others,
  and they fail with `SQLITE_BUSY` after 5 s. That comes from SQLite itself. Keep
  transactions short.
- **No auth.** Anyone who can reach the handler has full SQL access. Bind to
  localhost or put your own auth middleware in front. Cross-origin browser
  requests are rejected (no CORS, plus `http.CrossOriginProtection`). The
  bundled server, when bound to loopback, also rejects non-loopback `Host`
  headers to block DNS rebinding. If you embed the handler, add that check
  yourself.
- **Memory.** `/v3/cursor` streams rows as they are read. Pipeline and v1
  responses are built in memory, like sqld's: a huge `SELECT` costs that much
  RAM in your process.
- **Values.** Integers stay exact (int64; strings in Hrana). BLOBs are base64
  (`{"base64": "..."}` in v1). Inf/NaN become `null`. `rows_read` is the number
  of rows returned and `rows_written` the rows changed, not SQLite's scan
  counters as in sqld.
- **Compatibility with sqld.** Responses are compared with a real sqld (see
  Development); where sqld and the Hrana spec disagree, the handler follows
  sqld, which is what clients are tested against (e.g. an `error` batch
  condition on a skipped step is true). Known differences:
  - No protobuf encoding (`/v3-protobuf`) and no WebSocket; clients fall back
    to HTTP JSON.
  - `want_rows: false` returns no rows, as the spec says (sqld ignores it).
  - A statement followed by unparsable text (`SELECT 1; garbage`) fails with
    `SQL_MANY_STATEMENTS`; sqld runs the first part and drops the rest.
  - Error messages come from your SQLite driver. Codes follow sqld
    (`SQLITE_CONSTRAINT`, `SQLITE_UNKNOWN` for plain `SQLITE_ERROR`, ...) when
    the driver exposes them (modernc.org/sqlite does); otherwise
    `SQLITE_UNKNOWN`. Syntax errors are `SQLITE_UNKNOWN` instead of sqld's
    `SQL_PARSE_ERROR`.
  - `decltype` is upper-cased by the drivers (`DATETIME` for `DateTime`).
  - `describe` reports columns only for `SELECT`/`VALUES`/`EXPLAIN`, not for
    `PRAGMA` or `RETURNING`.
  - sqld rejects some statements its own parser does not support (e.g.
    `CREATE TEMP TABLE`); the handler passes everything to SQLite.
- **Close** interrupts running queries, rolls back every session and waits for
  in-flight requests.
- **Drivers.** Only `database/sql` is used, so any SQLite driver works, but
  the test suite runs with modernc.org/sqlite: its assertions on Hrana error
  codes and DATETIME values are specific to it (other drivers report
  `SQLITE_UNKNOWN`, and may return dates differently). The driver chooses
  how values come back: DATE/DATETIME/TIMESTAMP columns parsed into `time.Time`
  are returned as `YYYY-MM-DD HH:MM:SS[.fff]`, which is exact for SQLite's own
  format but normalizes other stored formats.

## Development

- `just test` runs the suite with `-race`; `just check` adds vet and gofmt.
- `just sqld` starts a real sqld in docker on `:8085`; `just test-sqld` then
  replays the v1, Hrana and cursor cases against both and compares the
  responses, and `just fuzz-sqld Values 5m` / `just fuzz-sqld Describe 5m`
  fuzz value encoding and SQL parameter parsing against it.
- `just test-clients` runs the official TypeScript and Go clients against a
  fresh handler (`just test-clients http://localhost:8085` runs them against
  sqld, to check the tests themselves).
