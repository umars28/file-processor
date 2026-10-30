---
description: Push the current branch and open a pull request to main
argument-hint: [task ID]
---

Open a pull request for the current branch. Task: $ARGUMENTS

1. If the current branch is `main`, stop and report.
2. If `git status --porcelain` is not empty, stop and report. Never commit on the human's behalf.
3. Run `make check`. If it fails, stop and report the failing targets.
4. Push with `git push -u origin <current-branch>`. Never push to `main`.
5. Create the PR with `gh pr create --base main`:
   - Title: conventional commit style, e.g. `feat(T3): worker pool and backpressure`.
   - Body sections: `## Summary`, `## Task` (task ID, AC checklist copied from `docs/tasks.md` with each item checked or unchecked), `## ADRs`, `## Checks` (one line per `make check` target).
   - No mention of Claude, AI attribution, or generated-by footers in the title or body.
6. Report the PR URL. Never merge it.
