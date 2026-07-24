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

## Insert conflict policy boundary

`--insert-conflict-policy` accepts `error`, `ignore-duplicate`, and
`replace-divergent-pk`. Generic target execution only ignores `1062` INSERT
errors under `ignore-duplicate`; it never performs replacement. Native ROW
`INSERT` duplicates under `ignore-duplicate` continue only after exact source/target
primary-key row equality. Under `replace-divergent-pk`, an unequal row is
replaced only for a `PRIMARY` duplicate when the source-PK lookup returns exactly
one row and the in-place primary-key UPDATE matches exactly one target row. Missing
or multiple lookup rows, zero/multiple matched rows, and update failures persist
evidence and abort without checkpoint advancement. The accepted overwrite risk is
explicit; replacement evidence is durable and a successful replacement can
checkpoint. Foreign-key, CHECK, and replacement-update conflicts persist evidence
and abort, rolling back the target transaction/checkpoint. Secondary-unique
conflicts follow that same path except the narrow superseded historical
`globalcomix.users`/`users.name` and `globalcomix.comics`/`comics.slug` ROW
`INSERT` proofs: exactly one candidate is allowed, and any ordinary conflict
mixed into the source transaction fails closed. The live stream reads `SHOW
MASTER STATUS` before one source consistent snapshot; that pre-snapshot
coordinate is a conservative lower bound and must be beyond the candidate
transaction. The users proof requires complete source/target PK and unique-owner
convergence from that snapshot plus active-transaction `SELECT ... FOR UPDATE`
reads. The comics proof requires complete current primary-row equality, while
accepting the locked unique owner by exact PK+slug identity despite unrelated
mutable-field drift. Both paths then require an existing same-file checkpoint
predecessor before the candidate and no later than the XID. Only then does it commit
remaining source-transaction rows, exact observation/resolution evidence, and
the XID checkpoint atomically. Any failed proof, predecessor, or commit rolls
back, then persists all unresolved observations independently; rollback or
persistence failures are surfaced. If a later conflict rolls back the enclosing
transaction, the replacement rolls back but the independent ledger evidence
remains. The default `error` policy fails native
row duplicates.

Snapshot/catchup writes may still use `INSERT IGNORE` where their configured
snapshot mode requests duplicate-ignore. Normal table-sync range repairs do
not: they use strict batched `INSERT` and surface constraint failures.

When table-sync insert or divergent-update batches receive a foreign-key error,
the repair target uses source/target schema-inventory FK metadata to discover
exact parent identities from the affected child rows. It recursively reads
source parents, compares target parents, inserts missing parents or updates
divergent parents, verifies each parent exactly, then retries only the failed
schema-dependent writer subbatch (capped at 128 rows and reduced by prepared-statement placeholder capacity). Nullable FK values are skipped. A concurrent
`1062` is reconciled by rereading the affected target rows: complete equality
with the source is accepted, while a divergent owner fails closed. After a
child insert, parent-retry, or update batch, every affected child row is
reread by primary key and compared exactly; only then may the caller checkpoint
the source chunk.

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

