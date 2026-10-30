# file-processor

Majoo take-home, Test Case 1: concurrent CSV processor in Go. Data: POS sales transactions from multiple outlets.

## Workflow

1. Design: `/adr <number> <topic>` → AI drafts context and options, a human fills in the Decision.
2. AC: `/ac <Tn>` → AI drafts acceptance criteria in `docs/tasks.md`, a human reviews and sets the status to `ac-approved`.
3. Build: `/task <Tn>` → AI implements with tests, then the `reviewer` agent checks the change against the ADRs and AC.
4. A human reads the verdict and the code, then commits manually.
5. Ship: `/pr <Tn>` → AI pushes the branch and opens a pull request to `main`. A human merges.

## Git workflow

- `main` is protected: no direct pushes, changes land only through pull requests.
- One branch per task or ADR, named `<type>/<scope>`, e.g. `feat/t3-worker-pool`, `docs/adr-0002-error-handling`, `chore/git-workflow`.
- Types: `feat`, `fix`, `test`, `docs`, `chore`, `refactor`, `perf`.
- Commit messages follow Conventional Commits.
- No Claude or AI attribution in commits, PR titles, or PR bodies.

## Rules for AI

- Never run `git commit`, rewrite git history, or merge pull requests.
- Never push to `main`. `git push` is allowed only for the current non-`main` branch, through `/pr`.
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
| `make hooks` | enables `.githooks/`: gitleaks on pre-commit, block pushes to `main` on pre-push |
