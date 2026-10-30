---
description: Implement one task against its AC, then run the reviewer
argument-hint: <Tn>
---

Work on task $ARGUMENTS from `docs/tasks.md`.

1. Check the task status. If it is not `ac-approved`, stop and report.
2. If `Owner: human`, do not write the implementation. Skip to step 6 to review the code the human wrote.
3. Read the task's AC and the ADRs it references. If any referenced ADR is still `proposed`, stop and report.
4. Write a test for each AC first, then implement until `make check` passes.
5. Do not commit. Do not modify ADRs or AC.
6. Invoke the `reviewer` agent with the task ID $ARGUMENTS.
7. Report in chat: files changed, test results, and the reviewer's verdict verbatim.
