# Live Row Error Handling

Native ROW/FULL streaming treats the MariaDB source as authoritative and the MySQL target as disposable. This spec defines live row-error behavior; out-of-band drift repair and historical conflict data remain separate workflows. See [Row Events](../row-events.md) and [Target Writer](../target-writer.md).

## What it must do

### Native inserts

- [x] Build a plain `INSERT` containing every writable source column, including the source primary key.
- [x] Treat MySQL `1062` from a native ROW `INSERT` as idempotent success.
- [x] Continue applying later statements in the same source transaction after an ignored INSERT `1062`.
- [x] Leave any divergent preexisting target row for out-of-band source/target convergence; live replay does not replace it.
- [x] Ignore the duplicate without reading, comparing, replacing, or updating any target row.
- [x] Ignore the duplicate without creating, resolving, or validating live conflict-ledger evidence.

### Other row errors

- [x] Propagate MySQL `1062` from ROW `UPDATE` or `DELETE`.
- [x] On MySQL `1452` from ROW `INSERT` or `UPDATE`, resolve the exact target constraint and fetch the exact same-schema parent row from the source.
- [x] Insert and recursively repair the parent chain inside the current target transaction, then retry the blocked row.
- [x] When an older native INSERT's exact parent key is absent, load its current source row by primary key.
- [x] Apply the current source INSERT through recursive repair without retrying historical values when its FK key changed.
- [x] Skip the historical INSERT when its current source row is absent; fail closed when the current row still requests the missing parent.
- [x] Keep this source-current substitution inside the missing-FK path so normal native INSERT `1062` remains read-free duplicate-ignore behavior.
- [x] When that repair-generated parent `INSERT` returns `1062`, resolve the exact non-prefix, non-expression unique index and lock exactly one conflicting target owner inside the same target transaction.
- [x] Update a same-primary-key owner to the intended source parent row without reinserting it.
- [x] For a different-primary-key owner, update it from its current source row or delete it when that source row no longer exists, then insert the intended parent.
- [x] Verify the intended parent values with binary-exact byte predicates before retrying the blocked child.
- [x] Fail closed on absent or ambiguous index metadata, zero or multiple owners, a remaining duplicate after owner reconciliation, or failed parent verification.
- [x] Bound missing-FK and duplicate-parent repair together to eight active repair keys and fail on a repeated key.
- [x] Propagate unrepaired `1452` and every other row error, including CHECK, schema, connection, and generated-column failures.
- [x] Roll back the complete source transaction after a propagated row error.
- [x] Do not commit or advance the transaction checkpoint after a propagated row error.

### Row identity

- [x] Apply updates by every before-image primary-key column while assigning writable after-image values.
- [x] Apply deletes by every before-image primary-key column.
- [x] Fail before target execution when required table-map or primary-key data is missing.

### Transaction execution

- [x] Apply live row changes serially on one initialized target connection.
- [x] Keep recursive missing-FK repair and duplicate-parent reconciliation inside
  that active target transaction.
- [x] Allow existing source transaction group-size and timeout controls to group
  complete source transactions without concurrent target workers.
- [x] Commit grouped target DML and its checkpoint atomically in source order.
- [x] Roll back the active target transaction and leave its checkpoint unchanged
  after any propagated row error.

### Out-of-band boundary

- [x] Keep staged `sync`, targeted conflict resolution, and historical `cdc.row_conflicts` data independent from live streaming.
- [x] Do not require `cdc.row_conflicts`, its trigger inventory procedure, or its grants to start the live stream.
- [x] Keep missing-parent repair source-authoritative and independent from conflict-ledger evidence.

## How it works

- [Row Events](../row-events.md)
- [Target Writer](../target-writer.md)
- [Live Stream Reconnect](live-stream-reconnect.md)

## Implementation inventory

- `src/row/apply.rs` — maps ROW events to typed target row changes.
- `src/row/sql.rs` — builds plain INSERT, before-key UPDATE, and key DELETE SQL.
- `src/mysql_client.rs` — applies serial ROW outcomes, grouped transactions,
  checkpoints, and source-authoritative repair on the sole target connection.
- `src/mysql_client/missing_foreign_key.rs` — orchestrates exact parent repair and shared depth/cycle bounds.
- `src/mysql_client/missing_foreign_key/duplicate_parent.rs` — resolves duplicate indexes and owners, plans source-authoritative owner changes, and verifies the intended parent.
- `src/mysql_client/missing_foreign_key/superseded_insert.rs` — loads a current source child when a historical INSERT's exact parent key no longer exists.
- `src/live/structured_stream/transaction.rs` — owns transaction rollback and checkpoint boundaries.
- `src/live/ddl_replay_journal/` — validates only live checkpoint and DDL-journal runtime contracts.

## Tests asserting this spec

- `src/mysql_client/tests.rs`
- `src/row/tests.rs`
- `src/live/structured_stream/tests/transaction.rs`
- `tests/cdc_eventual_consistency.rs`
- `scripts/cdc-integration-harness.py`

## Focused proof

- `missing-fk-nested-parent-auto-insert` exercises nested `sessions → guests → utms`
  repair through the serial live stream, including child retry and exact
  checkpoint completion.
- `missing-fk-duplicate-parent-reconcile` replays production-shaped `users.name`
  and `comics.slug` collisions through serial live apply, covering same-primary-key
  update, different-primary-key source update, source-absent owner deletion,
  child retry, and exact checkpoint completion.
- `missing-fk-superseded-insert` replays a historical `(comic_id, comic_format_id)`
  child after the source and preconverged target parent changed format, proving
  current-child substitution and exact serial checkpoint completion.

## Out of scope

- Concurrent live target workers, parallel target submission, and any
  `--target-parallel-transactions` option.
- General target-row equality checks, merge semantics, or automatic replacement outside repair-generated missing-FK parent/current-child insertion.
- Deleting historical conflict records or removing out-of-band repair commands.
