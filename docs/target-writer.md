# Target Writer

The target writer builds MySQL DML with ordered parameters and delegates actual
execution to a `TargetExecutor`.

Supported operations:

- batched insert for snapshot rows, with either upsert or duplicate-ignore mode
- update by primary key
- delete by primary key

The snapshot writer emits either a plain multi-row `INSERT`, an upsert, or an
ignore-duplicate insert according to its configured mode. The native ROW applier
uses a separate plain `INSERT` path and emits:

- `UPDATE ... SET ... WHERE <primary-key predicates>`
- `DELETE FROM ... WHERE <primary-key predicates>`

For a ROW update that changes its primary key, the assignments cover every
writable, non-generated after-image column and the predicates cover every
before-image primary-key column.

## Insert conflict policy boundary

`--insert-conflict-policy ignore-duplicate` affects generic target execution:
a MySQL `1062` is treated as success when the statement begins with `INSERT INTO`.
It also applies to native ROW changes: a duplicate from a ROW `INSERT` or
`UPDATE` is logged as skipped without durable conflict evidence, so the target
transaction and live checkpoint can commit. With the default `error` policy,
the native row duplicate fails, rolls back the transaction, and leaves the
checkpoint unchanged. Supported non-duplicate constraint conflicts still use
the durable conflict path.

Snapshot/catchup writes and normal range table repairs explicitly use
`INSERT IGNORE`; that SQL choice is independent of the flag. The
`sync-table --updated-since` path uses an upsert.

Errors include:

- operation name
- table name
- row count
- executor error
- SQL text

The executor trait keeps SQL generation testable without tying the core to a
specific MySQL client crate yet.

