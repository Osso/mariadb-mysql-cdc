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

## Commands

```bash
cargo run -- plan
cargo run -- probe --host 127.0.0.1 --user repl --password-env SOURCE_PASSWORD \
  --start-position 4 --stop-position 1000 --binlog-file mysql-bin.000001
```
