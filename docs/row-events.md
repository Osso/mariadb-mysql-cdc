# Row Events

Row events are applied through table-map metadata and the target writer.

## Metadata

`TableMapEvent` records the source table id, schema, table name, ordered column
names, and primary-key columns. The row applier keeps the latest map for each
table id, matching binlog behavior where later table-map events replace older
metadata for the same id.

## DML Mapping

The applier translates full row images into target DML:

- `WriteRowsEvent` becomes a plain `INSERT` with every writable, non-generated
  source column, including the source primary key.
- `UpdateRowsEvent` uses the after image for assignments. When the primary key
  changes, the statement assigns every writable, non-generated after-image
  column and predicates on every before-image primary-key column. When it does
  not change, only changed writable columns need assignment.
- `DeleteRowsEvent` uses every before-image primary-key column for `DELETE`.

Each row statement runs inside the target transaction. Duplicate-key and
supported constraint conflicts are recorded in the independent conflict ledger,
then returned as row failures; the target transaction and its live checkpoint are
not advanced. Repeating the same source event updates the same conflict record,
while a different source primary key gets a different record.

Primary-key values are extracted from the table map's primary-key columns. A row
event with no table map, no primary key, or a missing primary-key value fails
before reaching the target.

## Error Context

Every row apply error includes the binlog file/position. Target write failures
also include the operation, schema/table name, and target writer error.
