# Validation

Validation is modeled as three read-only commands over source and target
readers. The core logic is trait-backed so the SQL client binding can be added
without changing comparison behavior.

## Table Counts

`validate_table_counts` compares source and target row counts for each table and
returns one `CountComparison` per table.

## Sampled Checksums

`validate_sampled_checksums` asks each reader for deterministic checksum samples
using the same table, primary-key, selected-column, and sample-size request. It
reports only differing or missing samples.

## Row Divergence

`report_row_divergence` reads a bounded primary-key ordered window from source
and target. It reports:

- rows missing from the source
- rows missing from the target
- rows present in both with differing column values

Every request carries the table name, primary-key columns, selected columns,
optional `start_after` primary key, and limit so row-level reports can be paged.

## Lost-binlog recovery evidence

`recover-lost-binlog` requires two evidence phases:

1. **Preparation:** exact JSON authorization, source/checkpoint identity,
   the current source scope hash for this attempt, and an all-InnoDB scope. The
   source boundary is captured with ordinary non-locking MariaDB reads; recovery
   requires no `FLUSH TABLES WITH READ LOCK`, `UNLOCK TABLES`, `LOCK TABLES`, or
   `RELOAD`. Recovery data repair covers every current source-scope table even
   when target-only base tables exist; the generic `repair-drift` contract
   remains strict.
2. **Reconciliation and commit:** committed source reads supply the full-scope
   insert/update/delete/verify comparison without a long-lived cross-table
   repeatable-read snapshot. Recovery-only schema convergence then drops
   target-only base tables child-before-parent with normal foreign-key
   enforcement; cycles and source-table references to target-only parents fail
   closed. The captured coordinate is the replay boundary: source commits after
   it remain eligible for stream replay after checkpoint advancement. The final
   target table inventory must exactly equal this attempt's source inventory.
   Any skipped table, unsupported engine, unresolved conflict, schema
   difference, count/content mismatch, inventory mismatch, or failed
   attempt-scope proof blocks the checkpoint transition.

Every recovery record retains its immutable old checkpoint, exact historical
barrier, source identity, its own scope hash, operator, reason, and phase
evidence. A separately authorized replacement atomically marks the exact
prepared owner `abandoned` with server-generated evidence and inserts a new
`prepared` owner for the same exact checkpoint, barrier, and source identity;
the replacement may record a different current scope hash. All old identity,
scope, and prepared evidence remain durable. The historical journal row is
preserved. Abandoned history does not suppress the barrier; active-barrier
selection excludes it only after exact `committed` or `verified` ownership, and
those statuses are terminal. `committed` is an availability-first skip over
purged history, not proof that the skipped interval was replayed. The recovery
may be marked `verified` only after post-transition full schema/data validation
reports zero unresolved drift.

Production execution, restart health, and `verified` evidence remain open until
measured and recorded; this document does not claim recovery completion.
