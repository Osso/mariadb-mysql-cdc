# Target Writer

The target writer builds MySQL DML with ordered parameters and delegates actual
execution to a `TargetExecutor`.

Supported operations:

- batched insert for snapshot rows, with either upsert or duplicate-ignore mode
- strict batched insert for table-sync range repairs
- update by primary key
- delete by primary key

The snapshot writer emits either a plain multi-row `INSERT`, an upsert, or an
ignore-duplicate insert according to its configured mode. Table-sync apply and
missing-primary-key repairs select plain multi-row `INSERT`; only the explicit
`--updated-since` path selects upsert. The native ROW applier uses a separate
plain `INSERT` path and emits:

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

Every other native row error is returned to the transaction layer. Non-INSERT
`1062`, foreign-key, CHECK, schema, generated-column, and connection failures
roll back the complete source transaction and block its checkpoint.

`--insert-conflict-policy` controls generic statement execution and out-of-band
snapshot/table-sync behavior only. It does not select a native ROW live-stream
policy.

Snapshot/catchup writes may still use `INSERT IGNORE` where their configured
snapshot mode requests duplicate-ignore. That preserves an existing target row
for the copy operation only; source remains authoritative, and an explicit
out-of-band `repair-drift` run can converge target divergence and extras. Normal
table-sync range
repairs do not use `INSERT IGNORE`: they use strict batched `INSERT` and surface
constraint failures.

When table-sync insert or divergent-update batches receive a foreign-key error,
the repair target uses source/target schema-inventory FK metadata to discover
exact parent identities from the affected child rows. It recursively reads
source parents, compares target parents, inserts missing parents or updates
divergent parents, verifies each parent exactly, then retries only the failed
schema-dependent writer subbatch (capped at 128 rows and reduced by prepared-statement placeholder capacity). Nullable FK values are skipped. A concurrent
`1062` is reconciled by rereading the affected target rows: complete equality
with the source is accepted, while a divergent owner fails closed. When an
absent parent insert hits a secondary-unique owner under another primary key,
table-sync may restore that owner to its exact source row, reread the restored
owner, and retry the parent insert. This requires exactly one target owner, a
different primary key, one source row at that owner identity, and a different
source unique value; primary, unknown, absent, ambiguous, rightful, or
unverifiable owners fail closed, and retries are bounded by the table's unique
index count. After a child insert, parent-retry, or update batch, every affected
child row is reread by primary key and compared exactly; only then may the
caller checkpoint the source chunk.

The `sync-table --updated-since` path uses an upsert.

Table-sync parent-repair errors are explicit for missing source parents,
malformed or ambiguous identities, dependency cycles, source/target reads,
parent writes, child-batch retry, and post-write verification. Other insert
constraints remain ordinary repair errors and are not silently converted into
success. Apply and missing-primary-key runs retry bounded recoverable read,
duplicate, verification, progress, network, deadlock, and lock-timeout errors
without advancing unchanged durable progress. FK-aware apply runs require a
final zero-drift scan before durable completion.

Errors include:

- operation name
- table name
- row count
- executor error
- SQL text

The executor trait keeps SQL generation testable without tying the core to a
specific MySQL client crate yet.

