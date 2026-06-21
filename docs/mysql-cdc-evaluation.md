# mysql_cdc Evaluation

Evaluated crate: `mysql_cdc` `0.2.1`

Fixture source:

- `fixtures/mixed-binlog/mysql-bin.000001`
- `fixtures/mixed-binlog/mysql-bin.000002`

Fixture properties:

- MariaDB `11.4.12`
- `binlog_format=MIXED`
- query events
- row insert/update/delete events
- table map events
- GTID events
- DDL
- binlog rotate

## Result

`mysql_cdc::binlog_reader::BinlogReader` successfully parses the captured
MariaDB mixed binlog fixture and exposes the required data-changing event types:

- `MariaDbGtidEvent`
- `QueryEvent`
- `TableMapEvent`
- `WriteRowsEvent`
- `UpdateRowsEvent`
- `DeleteRowsEvent`
- `RotateEvent`

The fixture test is `tests/mysql_cdc_eval.rs`.

## Unsupported Event Types

The fixture also produces `UnknownEvent` for event type `161`, which
`mysql_cdc` names internally as `MariaDbBinlogCheckpointEvent`.

This is not a data-changing event. It should be classified and skipped by our
code instead of treated as a migration blocker.

## Required Patches or Wrappers

No patch is required for the captured fixture's required data-changing event
coverage.

Likely wrapper work before live use:

- Classify MariaDB binlog checkpoint event type `161` explicitly.
- Treat `UnknownEvent` as fatal unless its event type is on a reviewed skip list.
- Keep `mariadb-binlog` as an oracle for fixture comparison.

Known upstream limitations from `mysql_cdc` documentation:

- no SSL support
- no split-packet handling for packets of 16 MB or larger
- only standard auth plugins `mysql_native_password` and `caching_sha2_password`

These limitations affect live replication use, not offline fixture parsing.
