# file-processor

Majoo take-home, Test Case 1: concurrent CSV processor in Go. Data: POS sales transactions from multiple outlets.

## Workflow

1. Design: `/adr <number> <topic>` → AI drafts context and options, a human fills in the Decision.
2. AC: `/ac <Tn>` → AI drafts acceptance criteria in `docs/tasks.md`, a human reviews and sets the status to `ac-approved`.
3. Build: `/task <Tn>` → AI implements with tests, then the `reviewer` agent checks the change against the ADRs and AC.
4. A human reads the verdict and the code, then commits manually.

## Rules for AI

- Never run `git commit`, `git push`, or rewrite git history.
- Never work on a task whose status is not `ac-approved`.
- Never modify an ADR with status `accepted`. If the implementation needs to deviate, stop and report.
- Tasks marked `Owner: human` may only be reviewed, not written, by AI.
- Write tests first, then the implementation.
- Every package that starts goroutines must call `goleak.VerifyTestMain(m)` (`go.uber.org/goleak`) in its `TestMain`.
- `make check` must pass before a task is reported as finished.

## Commands

| Command | What it runs |
|---|---|
| `make test` | `go test -race` with coverage profile |
| `make cover` | `make test` + `coverage.txt`, `coverage.html`, fails below `COVERAGE_MIN` (80%) |
| `make lint` | `golangci-lint` incl. `govet`, `staticcheck`, `errcheck`, `gosec` |
| `make vuln` | `govulncheck` on dependencies |
| `make secrets` | `gitleaks` on the working tree and git history |
| `make check` | all of the above |
| `make hooks` | enables the gitleaks pre-commit hook in `.githooks/` |
