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
   before commit. The target is not re-inventoried and no post-write drift scan
   is performed. The captured coordinate remains the replay boundary: source
   commits after it remain eligible for stream replay after checkpoint
   advancement.

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
