# Catchup Snapshot

`catchup-snapshot` backfills a MariaDB source database into a MySQL target while
the live CDC stream keeps applying new changes. Operational usage is described
in [catchup.md](../catchup.md).

## What it must do

### Connection security

- [x] Treat the live GlobalComix source MariaDB (`source-mariadb.example` /
      `192.0.2.10`) as plaintext-only by accepted operational policy.
- [x] Do not require or pass a source TLS CA file for the current source; source
      transport is explicitly plaintext-only.
- [x] Use explicit source plaintext mode only for the current source; do not add
      opportunistic TLS-to-plaintext fallback behavior.
- [x] Require the target TLS CA file, validate its certificate chain, and use it
      for MySQL target writes.
- [x] Require certificate identity matching for DNS/hostname target endpoints.
- [x] Reject target plaintext, invalid-certificate acceptance, and target
      TLS-validation retry fallbacks; retries retain configured target CA and
      hostname validation.

### Snapshot Execution

- [x] Copy source rows into the target in deterministic primary-key order.
- [x] Resume completed tables and completed parallel ranges without recopying
  rows.
- [x] Use persistent source, target, and progress database connections during
  chunk copy work.
- [x] Keep shared MySQL TCP liveness bounds on those persistent connections:
  10-second TCP connect timeout and TCP keepalive beginning after 10 seconds
  idle; on Linux, probes run every 5 seconds for 3 attempts with a 30-second
  TCP user timeout. Apply 30-second read/write operation timeouts to catchup
  work only; live CDC/DDL connections use the shared liveness bounds without
  those operation timeouts.
- [x] Split large target inserts so MySQL prepared statements stay under the
  placeholder limit.
- [x] Create missing target tables from source DDL before row copy starts.
- [x] Refuse to copy when an existing target table is missing source columns.

### Execution concurrency contract

- [x] Run one catchup/snapshot synchronization at a time for this deployment.
- [x] Do not introduce fencing, coordination, or recovery behavior for
      concurrent independent snapshot/stream synchronizations; that is not a
      supported operational mode.
- [x] Parallel range workers, when enabled inside one catchup run, are not
      independent synchronizations and must not be treated as such.

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

### Progress-table privilege boundary

- [ ] Support catchup with an admin-prebootstrapped `cdc.table_sync_progress`
      and a runtime account limited to table DML. The current legacy catchup
      ensure path still attempts schema/table creation and a conditional
      `ALTER`, so prebootstrap alone does not remove its DDL requirement.

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
  MariaDB 11.4/MySQL 8.0 engine coverage for catchup copy behavior, target
  progress, and target CA validation in the disposable harness. The live
  GlobalComix source is plaintext-only, so this scenario must not be cited as a
  production source-TLS requirement. A completed rerun proves a no-op; it does
  not claim interrupted parallel-range resume.
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
