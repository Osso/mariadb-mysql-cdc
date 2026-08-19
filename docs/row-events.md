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

Each row statement runs inside the target transaction. A native ROW `INSERT` that
returns MySQL `1062` is treated as idempotent success. The stream does not read,
compare, replace, or repair the target row and does not write conflict-ledger
evidence. Later statements in the same source transaction continue.

MySQL `1452` from an INSERT or UPDATE triggers bounded source-authoritative
parent repair. The serial target executor resolves the exact target constraint,
fetches the exact same-schema parent row from the source, recursively installs
any missing parent chain inside the same target transaction, and retries the
blocked row.

If an older native INSERT's exact composite parent key no longer exists, the
serial target executor loads that child table's current source row by primary
key. A changed FK
causes the current row to be inserted through the same recursive repair path
without retrying the historical values. An absent current source row makes the
historical INSERT a no-op. A current row that still references the absent parent
fails closed.

If a repair-generated parent `INSERT` returns `1062`, the serial target
executor resolves the reported unique index and locks its single target owner in
that transaction. A same-primary-key owner is updated to the intended source
parent. A different owner is updated from its current source row, or deleted
when that source row no longer exists, before the intended parent is inserted.
The executor then verifies
the intended parent with binary-exact byte predicates before retrying the child.
Native ROW `INSERT` duplicates remain unchanged and never enter this path.

A missing source parent not resolved by the narrow superseded-INSERT rule,
cross-schema reference, prefix or expression index, ambiguous index or owner,
repeated repair key, combined repair depth beyond eight, remaining duplicate,
metadata failure, verification mismatch, or unsuccessful child retry is
transaction-fatal.

Every other row error is transaction-fatal. This includes non-`INSERT` `1062`,
`1452` that cannot be repaired, CHECK failures, schema mismatches that reach
execution, connection errors, and generated-column failures. The target
transaction rolls back and its checkpoint does not advance.

`--insert-conflict-policy` does not change native ROW streaming behavior. General
supersession, conflict-ledger, target-equality, and row-replacement paths do not
exist. Source-current INSERT substitution is limited to an exact parent lookup
that became stale after the source row changed or disappeared. The narrow
duplicate-owner reconciliation above exists only inside a repair-generated
missing-FK parent insertion. It neither reads nor
writes conflict-ledger evidence; explicit broad source-authoritative convergence
remains the staged `sync` operation.

Primary-key values are extracted from the table map's primary-key columns. A row
event with no table map, no primary key, or a missing primary-key value fails
before reaching the target.

## Error Context

Every row apply error includes the binlog file/position. Target write failures
also include the operation, schema/table name, and target writer error.
