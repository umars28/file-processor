COVERAGE_MIN ?= 80

.PHONY: test cover lint vuln secrets check hooks

test:
	go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...

cover: test
	go tool cover -func=coverage.out | tee coverage.txt
	go tool cover -html=coverage.out -o coverage.html
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub("%","",$$3); print $$3}'); \
	awk -v t="$$total" -v m="$(COVERAGE_MIN)" 'BEGIN { if (t+0 < m+0) { printf "coverage %.1f%% is below %s%%\n", t, m; exit 1 } printf "coverage %.1f%% (min %s%%)\n", t, m }'

lint:
	golangci-lint run ./...

vuln:
	govulncheck ./...

secrets:
	gitleaks dir . --redact --no-banner
	@if git rev-parse -q --verify HEAD >/dev/null; then gitleaks git . --redact --no-banner; fi

check: lint cover vuln secrets

hooks:
	git config core.hooksPath .githooks
