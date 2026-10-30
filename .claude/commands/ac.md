---
description: Draft acceptance criteria for one task
argument-hint: <Tn>
---

Draft acceptance criteria for task $ARGUMENTS in `docs/tasks.md`.

1. Read the task, the ADRs it references in `docs/adr/`, and `CLAUDE.md`.
2. Write 3–7 AC as a checklist under the task's `### AC` heading.
3. Every AC must be provable by a test or a command. Avoid vague words like "fast", "robust", or "efficient" without a number or condition.
4. Include at least one edge case and one failure case.
5. Set the task status to `ac-draft`. Never set it to `ac-approved`.
6. In chat, list the assumptions you made so they are easy to review.
