.PHONY: build test vet fmt verify run smoke clean

APP_PORT ?= 8080
RETENTION_LIMIT ?= 100
DATA_DIR ?= ./data

build:
	CGO_ENABLED=0 go build -buildvcs=false -o bin/server ./cmd/server
	CGO_ENABLED=0 go build -buildvcs=false -o bin/smoke ./cmd/smoke
	CGO_ENABLED=0 go build -buildvcs=false -o bin/restart-smoke ./cmd/restart-smoke
	CGO_ENABLED=0 go build -buildvcs=false -o bin/healthcheck ./cmd/healthcheck

vet:
	go vet ./...

fmt:
	gofmt -w cmd/ internal/

test:
	go test -race -count=1 ./...

# Full one-shot gate: build, unit/integration tests, e2e smoke and a real
# restart-durability check. Exit code is a bitmask (see scripts/verify.sh).
verify:
	APP_PORT=$(APP_PORT) RETENTION_LIMIT=25 scripts/verify.sh

run: build
	APP_PORT=$(APP_PORT) DATA_DIR=$(DATA_DIR) RETENTION_LIMIT=$(RETENTION_LIMIT) ./bin/server

smoke: build
	./bin/smoke -base http://localhost:$(APP_PORT)

clean:
	rm -rf bin/ data/ .verify-data/
