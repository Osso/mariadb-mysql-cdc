# Catchup Snapshot

`catchup-snapshot` backfills a MariaDB source database into a MySQL target while
the live CDC stream keeps applying new changes. Operational usage is described
in [catchup.md](../catchup.md).

## What it must do

### Connection security

- [x] Require a source TLS CA file, validate its certificate chain, and use it
      for the MariaDB source inventory and persistent snapshot reads.
- [x] Require the target TLS CA file, validate its certificate chain, and use it
      for MySQL target writes.
- [x] Fail before driver connection when either endpoint CA is missing,
      unreadable, empty, or invalid.
- [x] Require certificate identity matching for DNS/hostname endpoints while
      skipping hostname/IP identity matching only for literal IP endpoints.
- [x] Reject plaintext, invalid-certificate, and TLS-validation retry
      fallbacks; retries retain the configured CA and chain validation.

### Snapshot Execution

- [x] Copy source rows into the target in deterministic primary-key order.
- [x] Resume completed tables and completed parallel ranges without recopying
  rows.
- [x] Use persistent source, target, and progress database connections during
  chunk copy work.
- [x] Split large target inserts so MySQL prepared statements stay under the
  placeholder limit.
- [x] Create missing target tables from source DDL before row copy starts.
- [x] Refuse to copy when an existing target table is missing source columns.

### Parallel Progress

- [x] Split large tables into disjoint range checkpoints named
  `table_name#rangeN`.
- [x] Persist each range checkpoint with its own total row estimate so
  `sync-progress` can show per-range `total_rows`, percent progress, row rate,
  and ETA while the table is still running.
- [x] Persist the parent table total and completion state when all ranges finish.

### Observability

- [x] Log table starts, chunk progress, and table completion with table number,
  total table count, completed table count, chunk bounds, copied rows, and
  elapsed seconds.
- [x] Store catchup progress in the target CDC progress table so progress can be
  read without source database credentials.
- [x] Return `sync-progress` output in 1-2 seconds during active catchup by
  bounding live target reads and using a clearly marked stale cache fallback
  when the target read exceeds 1500ms.

## How it works

- [catchup.md](../catchup.md) documents operator commands and restart behavior.
- [table-sync-repair.md](table-sync-repair.md) covers row repair and divergence
  handling around catchup.

## Implementation inventory

- `src/mysql_snapshot.rs` - catchup command orchestration, table inventory, and
  progress persistence.
- `src/mysql_snapshot/parallel.rs` - parallel table range planning, range copy,
  and range progress totals.
- `src/mysql_snapshot/target_schema.rs` - target schema checks and missing table
  creation.
- `src/snapshot.rs` and `src/snapshot_ranges.rs` - chunk copy, checkpointed
  range requests, and range planning.
- `src/target.rs` - target MySQL writer and batched insert splitting.
- `src/sync_progress_cli.rs` - operator progress output, rates, percent, and
  ETA formatting.
- `src/table_sync/progress.rs` - target CDC progress table schema and upserts.

## Tests asserting this spec

- `scripts/cdc-integration-harness.py --scenario catchup-snapshot-tls` — real
  MariaDB 11.4/MySQL 8.0 connections using the configured CA on literal IP
  endpoints; proves CA/chain validation, four-row copy, target progress, and
  idempotent resume. This scenario does not claim DNS/hostname identity
  coverage.
- `src/mysql_snapshot/tests.rs`
- `src/mysql_snapshot/parallel.rs`
- `src/snapshot/tests.rs`
- `src/target.rs`
- `src/sync_progress_cli.rs`

## Known gaps (current cycle)

- [x] Add aggregate parent-table progress while parallel ranges are running.

## Out of scope

- Serving application traffic from the target during catchup.
- Automatic cutover.
