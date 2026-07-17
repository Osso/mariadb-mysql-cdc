# Recurring Drift Repair

`repair-drift` orchestrates bounded, run-scoped phased repairs after forward CDC
application. Each recurrence gets a fresh orchestration ID; only the exact
interrupted run may be resumed. The real Docker harness defines 32 executable
scenarios and proves FK ordering, fail-closed planning, resumable runs, PK-window
bounds, secondary-unique safety, and zero unresolved debt for the repaired scope.

## Current behavior

- [x] Generate a fresh orchestration run ID for each invocation.
- [x] Inventory source/target base tables and compare counts plus bounded content
      checks.
- [x] Skip missing/incompatible tables with explicit reasons.
- [x] Require an explicit `--max-deletes` in apply mode.
- [x] Pass child run IDs to `sync-table`.
- [x] Keep content-check bounds visible; at most 1,000 mismatch ranges are
      recorded and floating-point columns are skipped.
- [x] Keep target writes primary-key based.

## Wired phased behavior

- [x] Canonical source/target FK inventory drives child-first deletes and
      parent-first inserts; `NO ACTION` and `RESTRICT` normalize across engines.
- [x] Immutable plan hashes reject changed plans when reusing an interrupted run.
- [x] Cycles, FK inventory/schema mismatch, and delete ceilings block before
      target mutation.
- [x] `--start-after`/`--end-at` bound the selected PK window; apply mode always
      carries an explicit `--max-deletes` value.
- [x] Unresolved conflicts resolve only after verified equality, with run/evidence
      fields, and the real harness proves zero unresolved debt for scope.
- [x] Secondary-unique collisions remain primary-key scoped and do not mutate the
      conflicting owner row.
- [x] Execute DeleteExtras, InsertMissing, UpdateDivergent, then a non-mutating
      Verify phase. Verify rereads the full configured scope and fails on any
      missing, extra, or divergent row; conflict resolution runs only after zero
      Verify mismatches.
- [x] Individual MariaDB 11.4 → MySQL 8.0 Docker scenarios pass.

Remaining eventual-consistency gates are recurring scheduling from unresolved
conflicts, full-tree parity, and deployment/cutover proof. Until a scheduler exists,
operators must start each recurrence with a fresh bounded orchestration ID and
review the persisted child run states.

Out of scope: unbounded deletion and automatic cutover.
