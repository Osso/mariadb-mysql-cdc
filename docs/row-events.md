# Row Events

Row events are applied through table-map metadata and the target writer.

## Metadata

`TableMapEvent` records the source table id, schema, table name, ordered column
names, and primary-key columns. The row applier keeps the latest map for each
table id, matching binlog behavior where later table-map events replace older
metadata for the same id.

## DML Mapping

The applier translates row images into target DML:

- `WriteRowsEvent` becomes a batched target upsert.
- `UpdateRowsEvent` uses the after image for `UPDATE ... WHERE <primary key>`.
- `DeleteRowsEvent` uses the before image primary key for `DELETE`.

Primary-key values are extracted from the table map's primary-key columns. A row
event with no table map, no primary key, or a missing primary-key value fails
before reaching the target.

## Error Context

Every row apply error includes the binlog file/position. Target write failures
also include the operation, schema/table name, and target writer error.
