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

Skeleton only. The first real milestone is a read-only probe that connects to a
MariaDB source, records binlog coordinates, and classifies event types without
writing to a target.

## Commands

```bash
cargo run -- plan
```

