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
- [x] Bound repair to eight active parent edges and fail on a repeated repair key.
- [x] Propagate unrepaired `1452` and every other row error, including CHECK, schema, connection, and generated-column failures.
- [x] Roll back the complete source transaction after a propagated row error.
- [x] Do not commit or advance the transaction checkpoint after a propagated row error.

### Row identity

- [x] Apply updates by every before-image primary-key column while assigning writable after-image values.
- [x] Apply deletes by every before-image primary-key column.
- [x] Fail before target execution when required table-map or primary-key data is missing.

### Transaction execution

- [x] Keep `--target-parallel-transactions 1` serial.
- [x] With `N > 1`, lease one target connection per complete source transaction.
- [x] Preserve the full `TargetRowChange` through delayed worker execution.
- [x] Give each parallel worker its own source connection and perform parent inserts on the leased target transaction.
- [x] Send and drain parallel body statements individually so INSERT `1062` can be ignored, `1452` can be repaired, and later statements can continue only after success.
- [x] Commit parallel transactions and checkpoints in source order.
- [x] Stop later commits and checkpoint advancement when an earlier body or commit fails.

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
- `src/mysql_client.rs` — applies serial ROW outcomes and creates parallel workers.
- `src/mysql_client/missing_foreign_key.rs` — resolves exact parents and enforces recursive depth/cycle bounds.
- `src/live/parallel_target.rs` — drains row changes with delayed error handling and source-ordered commits.
- `src/live/parallel_writer.rs` — preserves full row metadata through worker submission.
- `src/live/submitted_mysql.rs` — owns each worker's submitted target connection, source connection, and lazy target-metadata connection.
- `src/live/structured_stream/transaction.rs` — owns transaction rollback and checkpoint boundaries.
- `src/live/ddl_replay_journal/` — validates only live checkpoint and DDL-journal runtime contracts.

## Tests asserting this spec

- `src/mysql_client/tests.rs`
- `src/row/tests.rs`
- `src/live/parallel_target_tests.rs`
- `src/live/structured_stream/tests/transaction.rs`
- `tests/cdc_eventual_consistency.rs`

## Focused proof

- `parallel-target-transactions` exercises concurrent real MariaDB/MySQL transactions, nested `sessions → guests → utms` repair, the ordered commit barrier, INSERT `1062` continuation, TLS, and exact checkpoint completion.
- Parallel pool tests prove a later prepared transaction cannot commit or publish a checkpoint after an earlier body failure.

## Out of scope

- Target-row equality checks, merge semantics, or automatic replacement.
- Deleting historical conflict records or removing out-of-band repair commands.
