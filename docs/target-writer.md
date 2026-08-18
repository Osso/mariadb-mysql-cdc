# Target Writer

The target writer builds MySQL DML with ordered parameters and delegates actual
execution to a `TargetExecutor`.

Supported operations:

- strict batched insert for unified source-authoritative sync chunks
- update by primary key
- delete by primary key

Unified sync emits strict plain multi-row `INSERT` statements. The native ROW
applier uses a separate plain `INSERT` path and emits:

- `UPDATE ... SET ... WHERE <primary-key predicates>`
- `DELETE FROM ... WHERE <primary-key predicates>`

For a ROW update that changes its primary key, the assignments cover every
writable, non-generated after-image column and the predicates cover every
before-image primary-key column.

## Live row-error boundary

Native ROW streaming always emits a plain `INSERT`. MySQL `1062` from that
INSERT is idempotent success without a target read, equality proof, replacement,
ledger write, or repair attempt. The stream continues with later statements in
the same source transaction.

MySQL `1452` from a native INSERT or UPDATE invokes bounded same-schema parent
repair. The executor resolves the exact target constraint, fetches the exact
parent row from the source, recursively inserts the parent chain in the current
target transaction, and retries the blocked row. Parallel workers retain the full
row metadata and own a source connection for this lookup.

Non-INSERT `1062`, unrepaired `1452`, CHECK, schema, generated-column, and
connection failures roll back the complete source transaction and block its
checkpoint. Repair also fails closed on absent source rows, cross-schema
references, repeated keys, or a chain deeper than eight.

`--insert-conflict-policy` is not part of unified `sync` and does not select a
native ROW live-stream policy. Unified sync never uses `INSERT IGNORE`, upsert,
`REPLACE`, post-write rereads, or final drift scans. Strict mutation errors fail
the locked chunk and leave its durable progress unchanged.

Schema constraints are prepared and restored by the staged schema phases. Row
chunks hold the target-table `WRITE` lock through source read, target read,
strict mutations, target commit, separate-session progress persistence, and
unlock. A lock or progress failure fails closed.

Errors include:

- operation name
- table name
- row count
- executor error
- SQL text

The executor trait keeps SQL generation testable without tying the core to a
specific MySQL client crate yet.

