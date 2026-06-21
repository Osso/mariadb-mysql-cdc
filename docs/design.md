# Design

## Problem

DigitalOcean Managed MySQL online migration supports MySQL 8 sources, but not
MariaDB sources. MariaDB and MySQL also differ at both the SQL layer and the
replication/binlog layer.

The tool should move data from MariaDB to a MySQL-compatible target without
requiring production to switch from mixed binlogs to row binlogs.

## Approach

Build a migration tool with two coordinated paths:

1. Snapshot existing data in deterministic chunks.
2. Consume MariaDB binlogs from the snapshot start point and apply compatible
   changes to the target.

The target is not trusted until rehearsals show low or zero divergence.

## Event Handling

The source may emit both statement and row events.

- Statement events are replayed only when known compatible with MySQL.
- Row events are applied as target DML once table metadata is available.
- Unsupported data-changing events stop the applier or enter quarantine with
  exact coordinates.
- DDL is initially treated as a cutover blocker, not an automatic operation.

## Parser Strategy

Start with the smallest reliable reader that can handle MariaDB mixed binlogs.
Candidates:

- `mysql_cdc` as a pure Rust replication client.
- `mariadb-binlog` as the initial read-only decoder and fallback subprocess.
- Captured binlog files as fixtures for parser compatibility tests.

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
