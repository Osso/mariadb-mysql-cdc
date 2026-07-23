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
`globalcomix.users` ROW `INSERT` on exact `users.name`: exactly one candidate is
allowed, and any ordinary conflict mixed into the source transaction fails
closed. The live stream reads `SHOW MASTER STATUS` before one source consistent
snapshot; that pre-snapshot coordinate is a conservative lower bound and must
be beyond the candidate transaction. It requires complete source/target PK and
unique-owner convergence from that snapshot plus active-transaction `SELECT ...
FOR UPDATE` reads, then requires an existing same-file checkpoint predecessor
before the candidate and no later than the XID. Only then does it commit
remaining source-transaction rows, exact observation/resolution evidence, and
the XID checkpoint atomically. Any failed proof, predecessor, or commit rolls
back, then persists all unresolved observations independently; rollback or
persistence failures are surfaced. If a later conflict rolls back the enclosing
transaction, the replacement rolls back but the independent ledger evidence
remains. The default `error` policy fails native
row duplicates.

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

