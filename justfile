# List recipes
default:
    @just --list

# Serve a local SQLite file to libSQL clients and DBeaver (pass e.g. -db x.db -addr 127.0.0.1:9000 -v)
run *args:
    go run ./cmd/server {{args}}

# Run all tests with the race detector
test *args:
    go test -race -count=1 ./... {{args}}

sqld_image := "ghcr.io/tursodatabase/libsql-server:v0.24.33"

# Start a real sqld in docker on :8085, the reference for test-sqld
sqld:
    docker run -d --rm --name libsql-handler-sqld -p 127.0.0.1:8085:8080 {{sqld_image}}
    @until curl -sf http://127.0.0.1:8085/version >/dev/null; do sleep 0.2; done
    @echo "sqld on http://127.0.0.1:8085 (stop with: just sqld-stop)"

sqld-stop:
    docker stop libsql-handler-sqld

# Compare responses (v1, Hrana, cursor) against a real sqld; needs `just sqld`
test-sqld url="http://localhost:8085":
    SQLD_URL={{url}} go test -count=1 -run 'MatchesRealSqld|Fuzz' ./tests

# Fuzz value encoding and SQL parameter parsing against sqld (target: Values or Describe)
fuzz-sqld target="Values" time="60s" url="http://localhost:8085":
    SQLD_URL={{url}} go test -run XXX -fuzz 'FuzzHrana{{target}}' -fuzztime {{time}} ./tests

# Run the official TS and Go clients against a fresh handler (or url=... e.g. sqld)
test-clients url="":
    #!/usr/bin/env bash
    set -euo pipefail
    url="{{url}}"
    if [ -z "$url" ]; then
        dir=$(mktemp -d)
        go build -o "$dir/server" ./cmd/server
        "$dir/server" -db "$dir/test.db" -addr 127.0.0.1:18099 2>"$dir/server.log" &
        pid=$!
        trap 'kill $pid 2>/dev/null; rm -rf "$dir"' EXIT
        # Wait for our server (not whatever else holds the port), at most 10 s.
        for _ in $(seq 50); do
            kill -0 $pid 2>/dev/null || { cat "$dir/server.log"; exit 1; }
            grep -q "Serving" "$dir/server.log" && curl -sf http://127.0.0.1:18099/version >/dev/null && break
            sleep 0.2
        done
        curl -sf http://127.0.0.1:18099/version >/dev/null || { echo "server did not start"; exit 1; }
        url=http://127.0.0.1:18099
    fi
    (cd tests/clients/ts && npx -y pnpm@latest install --silent && LIBSQL_URL=$url node --test)
    (cd tests/clients/go && LIBSQL_URL=$url go test -count=1 ./...)

# vet + tests; run before committing
check: && test
    go vet ./...
    test -z "$(gofmt -l .)" || (gofmt -l . && exit 1)

build:
    go build -o bin/server ./cmd/server

clean:
    rm -rf bin
