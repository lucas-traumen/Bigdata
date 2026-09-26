---
name: code-review
description: Perform independent read-only review of a completed diff using the approved plan, test evidence, and code-impact context.
---

# Code Review

## Inputs

- approved plan
- acceptance criteria
- Git diff
- tester result
- relevant project memory
- repository
- GitNexus when available

## Procedure

1. Review changed code, not coder intention.
2. Map changed symbols/modules.
3. Use GitNexus when available for callers, callees, dependencies, execution flows, blast radius.
4. Check correctness.
5. Check edge cases.
6. Check regression risk.
7. Check architecture consistency.
8. Check error handling.
9. Check security where relevant.
10. Check test coverage of affected behavior.
11. Do not modify code.
12. Rank findings by severity.

Output:

- verdict
- blockers
- major
- minor
- impact
- missing tests
- recommendation
