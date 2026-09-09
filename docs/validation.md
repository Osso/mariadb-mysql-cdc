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

1. **Preparation:** exact JSON authorization, source/checkpoint identity, the
   current source scope hash, and an all-InnoDB scope. The source boundary is
   captured as one coordinate plus one committed `SchemaSourceEvidence` set
   with ordinary non-locking MariaDB reads; recovery requires no `FLUSH TABLES
   WITH READ LOCK`, `UNLOCK TABLES`, `LOCK TABLES`, or `RELOAD`. Prepared
   evidence is source-only: scope hash, source schema fingerprint, and source
   table count.
2. **Unified reconciliation and commit:** the captured evidence drives one
   staged sync over every source table under the exact recovery ID. Unified
   prerequisite schema convergence, locked source-authoritative row chunks,
   durable `cdc.sync_runs` stage/table progress, and final constraints define
   successful reconciliation. Proof requires exactly one complete progress
   result for every expected source table, with no missing, unexpected,
   duplicate, incomplete, or wrong-run rows. The source scope hash is rechecked
   before commit, together with retention of the original binlog boundary.
   The target is not re-inventoried and no post-write drift scan
   is performed. The captured coordinate remains the replay boundary: source
   commits after it remain eligible for stream replay after checkpoint
   advancement.

Explicit prepared recovery resume adds the [prepared recovery identity, scope, and retention gates](checkpoints.md#prepared-recovery-resume-and-activation)
without weakening exact durable progress proof. Its process-kill harness also
proves that completed tables and scanned prefixes are not rescanned, while their
post-capture changes remain replayable from the original boundary.

`activate-lost-binlog` is the completed-rows exception to full final-constraint
convergence: it requires the same authorized existing `prepared` record, exact
unchanged source scope, retained prepared boundary, and read-only complete
prerequisite/`Rows` progress. It does not run DDL, access source/target table data,
rescan, capture a new boundary, modify source configuration, or change final FK
progress. Its atomic checkpoint/recovery-record transition records
`data_converged=true`, `schema_converged=false`, and
`final_constraints_deferred=true`; it is not final schema-convergence evidence.

Every recovery record retains its immutable old checkpoint, exact historical
barrier, source identity, its own scope hash, operator, reason, and preparation
evidence. A separately authorized replacement atomically marks the exact
prepared owner `abandoned` with server-generated evidence and inserts a new
`prepared` owner for the same exact checkpoint, barrier, and source identity;
the replacement may record a different current scope hash. All old identity,
scope, and prepared evidence remain durable. The historical journal row is
preserved. Abandoned history does not suppress the barrier; active-barrier
selection excludes it only after exact `committed` or `verified` ownership, and
those statuses are terminal. `committed` is an availability-first skip over
purged history, not proof that the skipped interval was replayed.

Production execution, restart health, and post-transition `verified` evidence
remain open until measured and recorded; this document does not claim recovery
completion.

## FK orphan repair proof boundary

`repair-fk-orphans` is outside staged recovery progress. Its disposable
`repair-fk-orphans-parents` scenario proves strict, source-authoritative restoration
for the ten explicit `comics_langs`/`releases` selectors, with constraints enabled
and no changed CDC control-plane rows. It does not prove a live repair or authorize
one. A live case must independently re-read its exact bounded count and selected-FK
absence, retain source/target evidence, and prove zero remaining identities without
writing recovery progress, checkpoints, journals, or source rows. See the [FK orphan
repair spec](specs/fk-orphan-repair.md).
