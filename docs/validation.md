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
   full schema convergence, complete source scope hash, and an all-InnoDB
   scope. The source boundary is read from one MariaDB `REPEATABLE READ`
   consistent snapshot opened behind a brief `FLUSH TABLES WITH READ LOCK`.
2. **Commit:** the same snapshot transaction supplies every full-scope
   insert/update/delete/verify comparison. Its coordinate comes from
   `SHOW MASTER STATUS` on that snapshot connection while the source write
   fence is held. Any skipped table, unsupported engine, unresolved conflict,
   schema difference, count/content mismatch, or scope-hash change blocks the
   checkpoint transition.

The committed record retains the old checkpoint, exact historical barrier, new
coordinate, source/scope identity, operator, reason, and measured evidence.
The historical journal row is preserved; only the exact committed barrier is
excluded from active-barrier selection. `committed` is an availability-first
skip over purged history, not proof that the skipped interval was replayed.
The recovery may be marked `verified` only after post-transition full
schema/data validation reports zero unresolved drift.

Production execution, restart health, and `verified` evidence remain open until
measured and recorded; this document does not claim recovery completion.
