# mariadb-mysql-cdc

Rust migration tooling for moving a MariaDB database to a MySQL-compatible
target with minimal downtime.

The immediate use case is MariaDB to DigitalOcean Managed MySQL, but this repo
is intentionally not tied to GlobalComix infrastructure. It should be usable as
a standalone migration/CDC tool.

## Design Constraints

- Consume production `binlog_format=ROW` with `binlog_row_image=FULL` so source
  primary keys and complete before/after row images are authoritative.
- Snapshot table data first, then stream MariaDB binlogs from a recorded
  position.
- Apply row changes by source primary key; a secondary-unique conflict must not
  mutate a different target primary key.
- Allow observable conflict skips so checksum-driven repair can provide eventual
  consistency without blocking the live stream.
- Stop or quarantine on unsupported data-changing events with exact binlog
  file/position/GTID.
- Keep the target out of service until repeated reconciliation proves data and
  schema parity.

## Current Status

The structured stream consumes native MariaDB row events and persists its
checkpoint in the target transaction. It automatically replays schema-changing
`QueryEvent` records approved by the MySQL compatibility policy. Recognized DDL
that is unsafe, MariaDB-only, or ambiguous becomes a manual boundary: the stream
flushes earlier DML, writes a pending row to a target-side DDL ledger, and stops
without checkpointing past the event.
Snapshot, drift-check, checksum localization, and primary-key table repair
commands support rehearsal and eventual convergence. Skipped duplicate conflicts
are observable reconciliation debt; the stream does not schedule repairs
automatically. The legacy `probe` text-binlog path is not a supported health check.

Target-side MySQL connections use TLS and load the DigitalOcean CA bundle from
`/etc/mariadb-mysql-cdc/do-ca.pem` when that file is mounted. The native source
binlog connection requires a CA file and verifies the pinned certificate; the
catchup source SQL connection currently does not use TLS. These paths are
rehearsal tooling, not evidence of production schema/data parity.

## DDL Resolution

The stream automatically executes DDL approved by its compatibility policy.
Recognized DDL rejected by that policy, or whose target schema is ambiguous, uses
the default `cdc.ddl_events` ledger; use `--ddl-ledger-table TABLE` to configure
another qualified table. An operator must review the recorded exact source SQL,
apply and validate the intended target schema change, then update the same ledger
row to `resolved` with a resolution note. On restart, the stream verifies the ledger raw SQL is an
exact match and advances the checkpoint without re-executing the DDL.

Generic target errors—including already-exists and missing-object errors—never
count as DDL success. Resolving before target apply and validation causes schema
divergence because the stream will checkpoint past the source DDL. Startup also
fails closed when the configured ledger schema, guards, trigger-inventory
routine, returned trigger metadata, or runtime grants do not match the bootstrap
contract. The restricted `cdc_stream` account lacks `TRIGGER` and receives only
`GRANT EXECUTE ON PROCEDURE cdc.ddl_events_trigger_inventory TO 'cdc_stream'@'%';`
for the exact `SQL SECURITY DEFINER` inventory routine; it never reads
`information_schema.triggers` directly. Bootstrap/resolver operators must
independently inspect the routine definition and actual trigger rows. See
[DDL Resolution Runbook](docs/ddl-resolution.md).

## Commands

```bash
cargo run -- plan
cargo run -- stream-binlog --source-host 127.0.0.1 --source-user repl \
  --source-password-env SOURCE_PASSWORD --source-database app \
  --source-tls-ca-file /etc/mariadb-mysql-cdc/source-ca.pem \
  --source-identity app-mariadb-20260710 \
  --binlog-file mysql-bin.000001 --start-position 4 \
  --target-host 127.0.0.1 --target-user cdc_stream \
  --target-password-env TARGET_PASSWORD --target-database app

cargo run -- sync-table --source-host 127.0.0.1 --source-user reader \
  --source-password-env SOURCE_PASSWORD --source-database app \
  --target-host 127.0.0.1 --target-user writer \
  --target-password-env TARGET_PASSWORD --target-database app \
  --table accounts --primary-key id --columns id,email,updated_at \
  --mode apply --run-id accounts-repair-20260710-01
```

`sync-table` requires `--run-id` and stores resumable run state in
`cdc.table_sync_runs` by default. Use a new ID for each recurrence; reuse an ID
only for the exact interrupted run, because completed IDs are terminal. The
immutable run specification covers source/target endpoints and databases, target
write policy, mode, primary-key range, chunk size, table shape, maximum deletes,
and `--updated-since` when present. A target-side named lock rejects concurrent
processes using the same run ID.

`repair-drift` runs the recurring bounded orchestration: it inventories source
and target tables, compares counts plus bounded content checks, creates a fresh
run ID, and invokes `sync-table` for count- or content-drifted tables with
compatible primary-key and column inventories. Content checks default to enabled
and can be disabled with `--content-check false`; they run only when source and
target counts match. Dry-run is the default. Apply mode requires an explicit
`--max-deletes` allowance; without it, orphan deletion remains disabled. Use
repeated `--table` options to limit scope and `--parent-first parent_a,parent_b`
to force a deterministic parent-first prefix before lexical ordering of remaining
tables. Each table repair receives a child run ID under the fresh orchestration ID.

Content checks split mismatches into primary-key ranges, but record at most 1,000
mismatch ranges; `range_limit_exceeded=true` means further splitting was bounded.
Floating-point columns are excluded from checksums because cross-server
normalization is unsafe; skipped columns are reported, so content parity is not
proven for those columns. Use `sync-table` with reviewed columns for targeted
repair/validation when needed.

```bash
mariadb-mysql-cdc repair-drift \
  --source-host 127.0.0.1 --source-user reader \
  --source-password-env SOURCE_PASSWORD --source-database app \
  --target-host 127.0.0.1 --target-user writer \
  --target-password-env TARGET_PASSWORD --target-database app \
  --mode apply --max-deletes 25 --parent-first accounts,authors
```

`cdc.table_sync_progress` remains the legacy catchup-only checkpoint table. An
interrupted `--updated-since` retry safely restarts from the beginning because a
row can become newly eligible behind a saved primary key; its idempotent upserts
never delete target orphans. Inspect a specific repair with:

```bash
mariadb-mysql-cdc sync-progress ... \
  --progress-table cdc.table_sync_runs \
  --run-id releases-repair-20260710-01 \
  --source-identity production-source
```

See [Catchup Workflow](docs/catchup.md) for the repair runbook and bounded-delete
rules.
