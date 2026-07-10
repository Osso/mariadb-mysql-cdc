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

The structured stream consumes native MariaDB row events, persists its checkpoint
in the target transaction, and replays allowlisted DDL query events. Snapshot,
drift-check, checksum localization, and primary-key table repair commands support
rehearsal and eventual convergence. The legacy `probe` text-binlog path is not a
supported health check.

## DDL Replay Support

DDL replay is allowlisted one operation at a time. Supported DDL is applied to
the MySQL target and treated as already applied when a retry sees the expected
idempotency error.

Current supported slices:

- Table/index DDL: `CREATE TABLE IF NOT EXISTS`, `ALTER TABLE`,
  `DROP TABLE IF EXISTS`, `TRUNCATE TABLE`, `RENAME TABLE`, `CREATE INDEX`,
  `CREATE UNIQUE INDEX`, and `DROP INDEX`, including MariaDB binlog QueryEvent
  text with semicolons inside SQL comments.
- Database/schema DDL: `CREATE DATABASE IF NOT EXISTS`, `ALTER DATABASE`,
  `DROP DATABASE IF EXISTS`, and their `SCHEMA` aliases. Retry idempotency
  covers `ERROR 1007` / `ERROR 1008`.
- View DDL: `CREATE VIEW`, `CREATE OR REPLACE VIEW`, `ALTER VIEW`, and
  `DROP VIEW IF EXISTS` when the statement does not contain unsafe definer
  clauses.
- Trigger DDL: `CREATE TRIGGER` and `DROP TRIGGER IF EXISTS` when the statement
  does not contain unsafe definer clauses, with retry idempotency for `ERROR
  1359` / `ERROR 1360`.
- Routine/event DDL: `CREATE`/`ALTER`/`DROP PROCEDURE`, `CREATE`/`ALTER`/`DROP
  FUNCTION`, and `CREATE`/`ALTER`/`DROP EVENT`, including compound bodies with
  semicolons. Retry idempotency covers `ERROR 1304` / `ERROR 1305` and `ERROR
  1537` / `ERROR 1539`.

The text `apply-binlog` extractor and structured stream path use the same
supported statement prefix list, so one-shot replays and live stream classify DDL
consistently.

Administrative DDL that should not be replayed into managed MySQL is supported
as a checkpointed skip: users, roles, grants, tablespaces, servers, and resource
groups advance the stream without mutating the target.

Unsupported or unsafe DDL still quarantines with exact binlog coordinates.

## Commands

```bash
cargo run -- plan
cargo run -- stream-binlog --source-host 127.0.0.1 --source-user repl \
  --source-password-env SOURCE_PASSWORD --source-database app \
  --target-host 127.0.0.1 --target-user writer \
  --target-password-env TARGET_PASSWORD --target-database app
```
