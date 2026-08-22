# Target Writer

The target writer builds MySQL DML with ordered parameters and delegates actual
execution to a `TargetExecutor`.

Supported operations:

- strict batched insert for unified source-authoritative sync chunks
- update by primary key
- delete by primary key

Unified sync emits strict plain multi-row `INSERT` statements. On a `1062`
from the named full-column non-`PRIMARY` secondary unique index, unified sync may
reconcile one proven wrong-primary-key owner inside the same target `WRITE` lock
and transaction. For each intended row, it resolves exactly one owner with
NULL-safe `<=>` predicates, exact-reads that owner primary key from current source,
updates it to the complete
source row or deletes it when source-absent, verifies the mutation and intended
row, then retries only the failed batch plus untouched remaining insert rows.
`PRIMARY`, prefixed, expression, absent, ambiguous, NULL-valued, repeated, and
source-legitimate-owner evidence fail closed. Normal inserts remain strict; no
ignore, upsert, replace, or fallback path exists. Repeated conflicts are bounded
and fail rather than loop.

The target chunk commits before durable progress is saved. Repair, retry,
verification, or commit failure rolls back the whole locked chunk and leaves
progress unchanged. Reconciliation audit events are secret-free, held pending
commit, emitted only after successful commit, and discarded on rollback or commit
failure.

The native ROW applier uses a separate plain `INSERT` path and emits:

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
target transaction, and retries the blocked row.

When an older native INSERT's exact parent key is absent, the executor queries the
current source child by primary key. It recursively inserts that current row when
its FK values changed, skips the historical INSERT when the current source row is
absent, and fails closed when the current row still references the missing key.
This repair-generated current INSERT may reconcile its own `1062`; the original
native INSERT duplicate-ignore rule is unchanged.

A `1062` from a repair-generated parent insert is not ignored. The executor
resolves the exact unique index, locks one target owner in the current
transaction, and reconciles that owner from source. It updates a same-primary-key
owner to the intended parent; for a different primary key, it updates the owner
from its source row or deletes it when source-absent, then retries the parent
insert. Exact parent values are verified before child retry. The sole serial target
connection performs owner reads, locks, parent repair, child retry, and the
surrounding transaction atomically.

Non-INSERT `1062`, unrepaired `1452`, CHECK, schema, generated-column, and
connection failures roll back the complete source transaction and block its
checkpoint. Repair also fails closed on absent source parents outside the narrow
superseded-INSERT rule, cross-schema references, unsupported or ambiguous
unique-index metadata, ambiguous owners, remaining duplicates, verification
failure, repeated keys, or combined repair depth beyond eight.

`--insert-conflict-policy` is not part of unified `sync` and does not select a
native ROW live-stream policy. Unified sync never uses `INSERT IGNORE`, upsert,
`REPLACE`, or a fallback engine. Its narrowly scoped secondary-unique repair uses
exact post-mutation rereads and retries only the failed plus untouched remaining
insert rows; strict mutation, repair, verification, and commit failures fail the
locked chunk and leave its durable progress unchanged. Final drift scans remain
outside the row-chunk boundary.

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

Live execution uses one initialized Rust `mysql::Conn`; source transaction
size and timeout controls may group complete source transactions, but no
concurrent target workers or parallel submission option exists. The executor
trait keeps SQL generation testable without exposing client-specific details to
the row-mapping core.

