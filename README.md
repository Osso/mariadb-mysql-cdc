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
checkpoint in the target transaction. Source schema-changing `QueryEvent` records
are a manual migration boundary: the stream flushes earlier DML, writes a pending
row to a target-side DDL ledger, and stops without checkpointing past the event.
Snapshot, drift-check, checksum localization, and primary-key table repair
commands support rehearsal and eventual convergence. The legacy `probe`
text-binlog path is not a supported health check.

## DDL Resolution

The stream never auto-executes source schema-changing DDL. The default ledger is
`cdc.ddl_events`; use `--ddl-ledger-table TABLE` to configure another qualified
table. An operator must review the recorded exact source SQL, apply and validate
the intended target schema change, then update the same ledger row to `resolved`
with a resolution note. On restart, the stream verifies the ledger raw SQL is an
exact match and advances the checkpoint without re-executing the DDL.

Generic target errors—including already-exists and missing-object errors—never
count as DDL success. Resolving before target apply and validation causes schema
divergence because the stream will checkpoint past the source DDL. See [DDL
Resolution Runbook](docs/ddl-resolution.md).

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
