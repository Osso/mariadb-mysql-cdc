# Recurring Drift Repair

`repair-drift` orchestrates bounded, run-scoped phased repairs after forward CDC
application. Each recurrence gets a fresh orchestration ID. Direct child-run
reuse remains limited to the exact interrupted run; an apply-mode InsertMissing
phase may reclaim exactly one failed `missing-primary-keys` run whose full
immutable specification matches. The real Docker harness includes executable
scenarios proving FK ordering, fail-closed planning, resumable runs, PK-window
bounds, secondary-unique safety, and zero unresolved debt for the repaired scope.

## Current behavior

- [x] Generate a fresh orchestration run ID for each invocation.
- [x] Inventory source/target base tables and compare counts plus bounded content
      checks.
- [x] When `--table` selects a subset, derive independent directional scopes:
      parentward ancestors for InsertMissing/UpdateDivergent and childward
      descendants for DeleteExtras; traversal never alternates through a shared
      node into siblings, and disconnected tables/constraints remain outside the
      repair scope.
- [x] Skip missing/incompatible tables with explicit reasons.
- [x] Reconcile target-only rows chunk by chunk in apply mode, verifying each
      deletion before persisting progress.
- [x] Pass child run IDs to `sync-table`.
- [x] Atomically claim exactly one specification-identical failed
      `missing-primary-keys` run during apply-mode InsertMissing within the table
      and immutable-specification scope; revalidate compatibility and uniqueness
      in a per-transaction `REPEATABLE READ` selection transaction with
      `FOR UPDATE` candidate reads before marking it running, exclude
      completed/incompatible runs, and fail closed on ambiguity without
      reclaiming a run.
- [x] Keep content-check bounds visible; at most 1,000 mismatch ranges are
      recorded and floating-point columns are skipped.
- [x] Keep target writes primary-key based.
- [x] In apply mode, `--conflict-reconcile-limit N` runs a bounded
      reconciliation-only cycle over at most `N` unresolved rows in the selected
      source/table scope. Read complete source and target rows by primary key and
      resolve only exact one-row equality with matching durable evidence. Missing,
      divergent, malformed, or ambiguous rows remain unresolved. The cycle performs
      no target-table repair, never reads or advances the stream checkpoint, and
      repeated cycles are idempotent.

## Wired phased behavior

- [x] Canonical source/target FK inventory drives child-first deletes and
      parent-first inserts; directional phase scopes keep sibling cycles out of
      unrelated selections; `NO ACTION` and `RESTRICT` normalize across engines.
- [x] Immutable plan hashes include the filtered source/target repair inventories
      and reject changed plans when reusing an interrupted run.
- [x] Cycles within either required directional phase scope and FK inventory/schema
      mismatch block before target mutation; disconnected cycles are ignored.
- [x] In apply mode, DeleteExtras processes one chunk at a time across the
      childward scope. Each chunk applies and verifies its writes and deletions,
      then persists progress. Interrupted child runs resume from the next
      uncommitted chunk; no global full-table delete preflight occurs. Read-only and repair
      inputs come from the full `plan.tables` union, so child-only descendants
      are deleted child-first.
- [x] `--start-after`/`--end-at` bound the selected PK window; completed chunks
      persist their cursor and counters only after exact verification.
- [x] Unresolved conflicts resolve only after verified equality, with run/evidence
      fields, and the real harness proves zero unresolved debt for scope.
- [x] Secondary-unique collisions remain primary-key scoped and do not mutate the
      conflicting owner row.
- [x] Execute DeleteExtras, InsertMissing, UpdateDivergent, then non-mutating
      verification over the observed repair scope. Tables with observed
      InsertMissing/UpdateDivergent reports receive full equality Verify; delete-only
      descendants with observed deletes verify only that no target extras remain.
      Source-only rows in delete-only descendants remain outside equality because
      InsertMissing is intentionally excluded there.
- [x] Individual MariaDB 11.4 → MySQL 8.0 Docker scenarios pass.

Remaining eventual-consistency gates are recurring scheduling from unresolved
conflicts, automatic parent-aware admission for missing-row conflicts, full-tree
parity, and deployment/cutover proof. Until a scheduler exists, operators must start
each recurrence with a fresh bounded orchestration ID and review the persisted child
run states. Exact-equivalent reconciliation is available as a bounded
reconciliation-only cycle; reclamation occurs only when an invoked repair cycle
encounters one compatible failed missing-PK child. Neither behavior is scheduling.

Out of scope: unbounded deletion and automatic cutover.
