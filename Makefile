.PHONY: run test build clean

run:
	go run cmd/server/main.go

test:
	go test -v ./...

build:
	mkdir -p bin
	go build -o bin/server cmd/server/main.go

clean:
	rm -rf bin local.db
