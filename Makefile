COVERAGE_MIN ?= 80
TEST_DATABASE_URL ?= postgres://fileproc:$${POSTGRES_PASSWORD:-fileproc}@127.0.0.1:$${POSTGRES_PORT:-55432}/fileproc?sslmode=disable

.PHONY: test test-integration cover lint vuln secrets check hooks bench latency

test:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...

test-integration:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -count=1 -v ./internal/store/...

cover: test
	go tool cover -func=coverage.out | tee coverage.txt
	go tool cover -html=coverage.out -o coverage.html
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub("%","",$$3); print $$3}'); \
	awk -v t="$$total" -v m="$(COVERAGE_MIN)" 'BEGIN { if (t+0 < m+0) { printf "coverage %.1f%% is below %s%%\n", t, m; exit 1 } printf "coverage %.1f%% (min %s%%)\n", t, m }'

lint:
	go tool -modfile=tools/go.mod golangci-lint run ./...

vuln:
	go tool -modfile=tools/go.mod govulncheck ./...

secrets:
	gitleaks dir . --redact --no-banner
	@if git rev-parse -q --verify HEAD >/dev/null; then gitleaks git . --redact --no-banner; fi

check: lint cover vuln secrets

bench:
	go test -run '^$$' -bench . -benchmem -count=5 ./internal/pipeline/

latency:
	go test -run TestCancelLatencyWithManyKeys -count=1 -v ./internal/pipeline/

hooks:
	git config core.hooksPath .githooks
