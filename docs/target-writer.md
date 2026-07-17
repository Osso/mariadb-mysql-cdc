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

Errors include:

- operation name
- table name
- row count
- executor error
- SQL text

The executor trait keeps SQL generation testable without tying the core to a
specific MySQL client crate yet.

