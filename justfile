# List recipes
default:
    @just --list

# Serve a local SQLite file for DBeaver (pass e.g. -db x.db -addr 127.0.0.1:9000 -v)
run *args:
    go run ./cmd/server {{args}}

# Run all tests with the race detector
test *args:
    go test -race -count=1 ./... {{args}}

# Compare responses against a real sqld (default http://localhost:8085)
test-sqld url="http://localhost:8085":
    SQLD_URL={{url}} go test -count=1 -run TestMatchesRealSqld -v ./tests

# vet + tests; run before committing
check: && test
    go vet ./...
    test -z "$(gofmt -l .)" || (gofmt -l . && exit 1)

build:
    go build -o bin/server ./cmd/server

clean:
    rm -rf bin
