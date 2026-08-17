# Catchup Workflow

Catchup uses a snapshot plus forward CDC replay:

1. Record a binlog boundary.
2. Snapshot source tables in primary-key chunks.
3. Replay events from that boundary in binlog order.
4. Continue streaming to the live tail.
5. Validate counts, stable primary-key windows, content, checksums, orphans, and
   schema state.

This is not a parity proof by itself. Snapshot `INSERT IGNORE` preserves any
conflicting target row, but does not overwrite divergent values or remove target
orphans. Snapshot behavior is independent from native live ROW streaming.

For native live ROW events, the source is authoritative and the target is
disposable. A plain INSERT that returns MySQL `1062` is accepted without target
inspection or repair. Every other row error rolls back the complete source
transaction and blocks checkpoint advancement. Offline `sync-table` and
`repair-drift` remain responsible for explicit convergence and verification.

Catchup source reads and target writes follow the [connection policy](schema-inventory.md#connection-policy).
The live source MariaDB (`source-mariadb.example` / `192.0.2.10`) is
plaintext-only; `catchup-snapshot`, `sync-table`, and `repair-drift` must not
require or pass a source CA for that source. Target writes continue to require
the reviewed DigitalOcean CA at `/etc/mariadb-mysql-cdc/do-ca.pem` plus target
hostname verification. The deployment must keep target CA material mounted while
omitting source CA arguments for the current source. The second successful
catchup invocation proves a completed-run no-op, not an interrupted
parallel-range resume.

For a resumable snapshot backfill:

```bash
mariadb-mysql-cdc catchup-snapshot \
  --source-host 192.0.2.10 \
  --source-user cdc_reader \
  --source-password-env SOURCE_PASSWORD \
  --source-database globalcomix \
  --target-host target-mysql.example \
  --target-port 25060 \
  --target-user target_user \
  --target-password-env TARGET_PASSWORD \
  --target-database globalcomix \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem \
  --progress-file /var/lib/mariadb-mysql-cdc/snapshot-progress.json \
  --chunk-size 10000 \
  --parallel-workers 4
```

## Progress-table privilege boundary

Repair commands default to `cdc.table_sync_runs`. Runtime `ensure` validates
its complete 16-column schema and `run_id` primary key through
`information_schema`; an administrator-prebootstrapped current table therefore
needs only `SELECT, INSERT, UPDATE` at runtime. An exact legacy 15-column table
is migrated once by adding the `delete_preflight_complete` column as
`BOOLEAN NOT NULL DEFAULT FALSE`, so that upgrade path requires `ALTER` and
preserves existing runs as requiring preflight. A missing table uses the
creation path and a malformed existing table fails unchanged.

`catchup-snapshot` defaults to the legacy `cdc.table_sync_progress` table. Its
current ensure path still creates the schema/table and conditionally adds
`total_rows`, so prebootstrapping this table does not yet make catchup
least-privilege. Do not claim catchup support with a table-DML-only runtime
account until that path is changed and proved.

## Recurring drift repair

`repair-drift` creates a fresh orchestration ID, inventories both endpoints, runs
bounded count/content checks, and invokes FK-aware phased `sync-table` repairs.
Dry-run is default. Apply mode reconciles target-only rows in dependency-safe
chunks, verifies each chunk, and persists progress after verification.
`--start-after`/`--end-at` select a bounded primary-key window; JSON forms support
comma-containing keys.

The planner deletes child rows before parents, inserts parents before children,
blocks cycles/schema mismatch before mutation, rejects changed plan hashes on
resume, and resolves conflict rows only after verified equality with evidence.

```bash
mariadb-mysql-cdc repair-drift \
  --source-host 192.0.2.10 --source-user cdc_reader \
  --source-password-env SOURCE_PASSWORD --source-database globalcomix \
  --target-host target-mysql.example \
  --target-port 25060 --target-user target_user \
  --target-password-env TARGET_PASSWORD --target-database globalcomix \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem \
  --mode apply --parent-first users,applications_users
```

Use this only after reviewing the exact target extras and FK constraints. Do not
assume lexical order can satisfy parent-first inserts and child-first deletes.

## Table repair runs

Every `sync-table` invocation requires `--run-id`. Direct reuse is limited to the
exact interrupted immutable run; a completed ID is terminal. A changed endpoint, table shape, bounds, mode, or `updated-since` specification
needs a fresh ID. During apply-mode InsertMissing, `repair-drift` may reclaim exactly
one failed missing-PK run only when its complete immutable specification matches.
Reclamation is an atomic claim scoped to the table and immutable specification:
compatibility and uniqueness are revalidated before the selected row is marked
running; multiple compatible candidates fail closed without reclaiming a run.
The claim acquires a table/specification-scoped `GET_LOCK`, then uses a target
transaction explicitly set to `REPEATABLE READ`; candidate enumerations use
`FOR UPDATE`, the selected row is marked `running` and committed, and only then
is the advisory lock released. Any failure rolls back the selection transaction
before lock release.

Apply mode preflights target extras before mutating. For `All` and
`DeleteExtras`, a successful preflight sets `delete_preflight_complete` on the
run row before repair starts; reconnects skip that completed scan, while failed
or incomplete preflights remain unmarked and rerun. If extras exceed the
explicit ceiling, it performs zero inserts, updates, or deletes. Normal
`sync-table` repair never deletes or updates by a secondary key.

## Completion gates

- [ ] Recurring repair scheduling from offline unresolved conflicts.
- [x] FK-aware real-MySQL phased repair with crash/resume proof for the repaired scope.
- [x] Zero unresolved conflict debt after verified equality for the repaired scope.
- [ ] Stable checkpoint/lag, schema parity, and no quarantine/manual/journal debt.
