---
name: implement-plan
description: Implement an approved task plan safely, stay inside scope, write relevant tests, and maintain recoverable implementation state.
---

# Implement Plan

## Preconditions

- `.ai/plans/current-plan.md` is user-approved.
- Current task status permits implementation.

## Procedure

1. Read `AGENTS.md`.
2. Read `.ai/plans/current-plan.md`.
3. Read `.ai/state/current-task.md`.
4. Inspect `git status` and `git diff`.
5. Read relevant project memory.
6. Use GitNexus when available.
7. Implement plan incrementally.
8. Keep one coder as owner of production changes.
9. Add/update tests required by plan.
10. At meaningful checkpoints, update task state when permitted.
11. Run focused checks if useful.
12. Do not commit or push automatically.
13. Return structured implementation report.

If approved plan is materially wrong or impossible, stop and report conflict to orchestrator. Do not silently redesign the task.
