# Design

## Problem

DigitalOcean Managed MySQL online migration supports MySQL 8 sources, but not
MariaDB sources. MariaDB and MySQL also differ at both the SQL layer and the
replication/binlog layer.

The tool moves data from MariaDB to a MySQL-compatible target. Production
streaming requires `binlog_format=ROW` and `binlog_row_image=FULL`; the source
must not be switched from the existing MariaDB `MIXED` format until the CDC
migration window explicitly enables and verifies that stream contract.

## Approach

Build a migration tool with two coordinated paths:

1. Snapshot existing data in deterministic chunks.
2. Consume MariaDB binlogs from the snapshot start point and apply compatible
   changes to the target.

The target is not trusted until rehearsals show low or zero divergence.

## Event Handling

Offline fixtures and `apply-binlog` may contain statement and row events.
Production `stream-binlog` requires ROW/FULL, while still treating source DDL
`QueryEvent` boundaries manually.

- Compatible statements are replayed only by offline/application paths.
- Row events are applied as target DML once table metadata is available.
- Unsupported data-changing events stop the applier or enter quarantine with
  exact coordinates.
- DDL is initially treated as a cutover blocker, not an automatic operation.

## Parser Strategy

`stream-binlog` uses the vendored `mysql_cdc` native replication client with
verified TLS. Captured binlog files remain parser compatibility fixtures;
`mariadb-binlog` text decoding belongs only to explicit offline/apply workflows.

The first probe uses `mariadb` for `SHOW MASTER STATUS` and `mariadb-binlog`
for read-only remote binlog streaming. It classifies event text into broad
categories so rehearsals can show which MariaDB event types appear before the
tool starts applying anything to a target.

## Safety

- Checkpoint every committed target transaction.
- Make writes idempotent where possible.
- Validate table counts and sampled checksums during rehearsal.
- Keep exact binlog coordinates in every error.

Checkpoint state is stored as JSON with source file/position, GTID, event
timestamp, and the last successfully processed event. See
`docs/checkpoints.md`.

Schema inventory is captured before snapshot/apply work so the migrator knows
primary keys, generated columns, object definitions, and source-side objects
that may need compatibility review. See `docs/schema-inventory.md`.

Snapshot export/import is modeled as deterministic primary-key chunks with
per-table progress. The source and target I/O are traits so the chunking and
resume semantics can be tested before database-specific readers/writers are
filled in. See `docs/snapshot.md`.

Target writes are generated as parameterized MySQL statements and executed
through a trait-backed writer. Snapshot rows use batched upserts, while CDC
updates/deletes use primary-key predicates. See `docs/target-writer.md`.

Statement events pass through a conservative allowlist before replay. Narrow
DML is replayed; source schema-changing DDL is recorded in a manual-resolution
ledger and stops the stream before its checkpoint, while MariaDB-only syntax,
unsafe file/definer patterns, and unknown non-DDL statement types are quarantined
with source coordinates. See `docs/statement-events.md` and
`docs/ddl-resolution.md`.

Row events are applied from table-map metadata. Each insert is a plain target
`INSERT` with the explicit source primary key, updates use after images, and
deletes use primary-key values from
before images. Missing table maps or primary-key values fail with source
coordinates. See `docs/row-events.md`.

Validation is split into read-only count checks, deterministic sampled checksum
checks, and paged row-level divergence reports. See `docs/validation.md`.

The rehearsal workflow runs snapshot, CDC apply, and validation against a target
that is explicitly guarded from serving application traffic. See
`docs/rehearsal.md`.

Cutover stops writes, drains CDC lag, validates again, switches the application
endpoint, and resumes writes. Failed pre-switch cutovers attempt to resume
writes without switching traffic. See `docs/cutover.md`.

Live streaming must reconnect after transient source connection loss and resume
from durable checkpoints instead of static startup coordinates. See
`docs/specs/live-stream-reconnect.md`.
