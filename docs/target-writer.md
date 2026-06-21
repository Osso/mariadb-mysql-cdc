# Target Writer

The target writer builds MySQL DML with ordered parameters and delegates actual
execution to a `TargetExecutor`.

Supported operations:

- batched insert/upsert for snapshot rows
- update by primary key
- delete by primary key

The writer emits:

- `INSERT ... VALUES (...), (...) ON DUPLICATE KEY UPDATE ...`
- `UPDATE ... SET ... WHERE <primary-key predicates>`
- `DELETE FROM ... WHERE <primary-key predicates>`

Errors include:

- operation name
- table name
- row count
- executor error
- SQL text

The executor trait keeps SQL generation testable without tying the core to a
specific MySQL client crate yet.

