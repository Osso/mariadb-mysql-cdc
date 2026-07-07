# mariadb-mysql-cdc

Rust migration tooling for moving a MariaDB database to a MySQL-compatible
target with minimal downtime.

The immediate use case is MariaDB to DigitalOcean Managed MySQL, but this repo
is intentionally not tied to GlobalComix infrastructure. It should be usable as
a standalone migration/CDC tool.

## Design Constraints

- Keep the source server on its existing `binlog_format=MIXED`.
- Do not require switching production replication to row-based binlogs.
- Snapshot table data first, then stream MariaDB binlogs from a recorded
  position.
- Apply known-compatible changes to the MySQL target.
- Stop or quarantine on unsupported data-changing events with exact binlog
  file/position/GTID.
- Keep the target out of service until repeated rehearsals prove compatibility.

## Current Status

The first read-only probe is available. It connects to a MariaDB source, records
the current binlog coordinates with `SHOW MASTER STATUS`, then uses
`mariadb-binlog` to read and classify events without writing to a target.

## DDL Replay Support

DDL replay is allowlisted one operation at a time. Supported DDL is applied to
the MySQL target and treated as already applied when a retry sees the expected
idempotency error.

Current supported slices:

- `CREATE TABLE IF NOT EXISTS ...` including MariaDB binlog QueryEvent text with
  semicolons inside SQL comments.
- `CREATE DATABASE IF NOT EXISTS ...` with retry idempotency for `ERROR 1007`
  (`database exists`).

Unsupported or unsafe DDL still quarantines with exact binlog coordinates.

## Commands

```bash
cargo run -- plan
cargo run -- probe --host 127.0.0.1 --user repl --password-env SOURCE_PASSWORD \
  --start-position 4 --stop-position 1000 --binlog-file mysql-bin.000001
```
