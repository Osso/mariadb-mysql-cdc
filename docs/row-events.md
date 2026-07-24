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

Each row statement runs inside the target transaction. Supported constraint
conflicts are recorded in the independent conflict ledger, then returned as row
failures; the target transaction and its live checkpoint are not advanced.
Repeating the same source event updates the same conflict record, while a
different source primary key gets a different record.

`--insert-conflict-policy` accepts `error`, `ignore-duplicate`, and
`replace-divergent-pk`. For a ROW `INSERT`, `ignore-duplicate` continues without
ledger evidence only when the target row fetched by source primary key exactly
equals the source row. `replace-divergent-pk` replaces an unequal row only when
MySQL reports `PRIMARY`, using a primary-key UPDATE of the source image; it
records durable replacement evidence and allows checkpoint advancement. The
accepted risk is overwriting the divergent target row. Foreign-key, CHECK, and
replacement-update conflicts persist evidence and abort. Secondary-unique
conflicts do too, except the narrow superseded historical
`globalcomix.users`/`users.name` and `globalcomix.comics`/`comics.slug` ROW
`INSERT` proofs: exactly one candidate is allowed, and any ordinary conflict
mixed into the source transaction fails closed. The stream reads `SHOW MASTER
STATUS` before one source consistent snapshot; that pre-snapshot coordinate is a
conservative lower bound and must be beyond the candidate transaction. The users
proof requires complete source/target convergence for the historical PK and
current unique owner using active-transaction `SELECT ... FOR UPDATE`. The
comics proof requires complete current primary-row equality, while accepting the
locked unique owner by exact PK+slug identity despite unrelated mutable-field
drift. Both paths then require an existing same-file checkpoint predecessor
before the candidate and no later than the XID. Only then does it commit the remaining
source-transaction rows, exact observation/resolution evidence, and XID
checkpoint atomically. Any proof, predecessor, or commit failure rolls back,
then persists all unresolved observations independently; rollback or persistence
failures are surfaced. If a later conflict rolls back the target transaction,
the replacement rolls back but its independent ledger observation survives. Supported non-duplicate constraint
conflicts remain durable repair debt regardless of this policy.

`--insert-conflict-policy ignore-duplicate` applies to this native ROW path.
A MySQL `1062` from either a ROW `INSERT` or `UPDATE` is logged as skipped,
without durable conflict evidence, so the target transaction and checkpoint can
advance. With the default `error` policy, the duplicate fails the row event and
blocks checkpoint advancement. Supported non-duplicate constraint conflicts
remain durable repair debt regardless of this policy.

Primary-key values are extracted from the table map's primary-key columns. A row
event with no table map, no primary key, or a missing primary-key value fails
before reaching the target.

## Error Context

Every row apply error includes the binlog file/position. Target write failures
also include the operation, schema/table name, and target writer error.
