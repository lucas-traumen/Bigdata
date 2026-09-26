---
name: create-plan
description: Convert user requirements and current repository evidence into a scoped implementation plan with acceptance criteria and required tests.
---

# Create Plan

## Inputs

Read:

- `AGENTS.md`
- `.ai/memory/PROJECT.md`
- `.ai/memory/DECISIONS.md`
- `.ai/memory/CONVENTIONS.md`
- `.ai/memory/KNOWN_ISSUES.md`
- current repository state
- GitNexus when available
- relevant user requirements from current session

## Procedure

1. Restate goal.
2. Establish actual current state from repository evidence.
3. Identify target state.
4. Define scope.
5. Define out-of-scope.
6. Identify relevant modules/symbols.
7. Use GitNexus for dependencies/flows when available.
8. Record task-level architecture decisions.
9. Produce ordered implementation steps.
10. Define constraints.
11. Define objective acceptance criteria.
12. Define required verification.
13. Identify risks/open questions.
14. Write `.ai/plans/current-plan.md`.
15. Set `.ai/state/current-task.md` to `USER_APPROVAL_REQUIRED`.

Do not implement production code.

The plan must be self-contained enough for a fresh coder child session to execute without receiving the full parent conversation.
