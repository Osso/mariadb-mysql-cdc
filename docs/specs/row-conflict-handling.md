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
- [x] Propagate every non-`1062` row error, including foreign-key, CHECK, schema, connection, and generated-column failures.
- [x] Roll back the complete source transaction after a propagated row error.
- [x] Do not commit or advance the transaction checkpoint after a propagated row error.

### Row identity

- [x] Apply updates by every before-image primary-key column while assigning writable after-image values.
- [x] Apply deletes by every before-image primary-key column.
- [x] Fail before target execution when required table-map or primary-key data is missing.

### Transaction execution

- [x] Keep `--target-parallel-transactions 1` serial.
- [x] With `N > 1`, lease one target connection per complete source transaction.
- [x] Send and drain parallel body statements individually so only INSERT `1062` can be ignored and later statements can continue.
- [x] Commit parallel transactions and checkpoints in source order.
- [x] Stop later commits and checkpoint advancement when an earlier body or commit fails.

### Out-of-band boundary

- [x] Keep snapshot/table-sync insert modes, drift repair, targeted conflict resolution, and historical `cdc.row_conflicts` data independent from live streaming.
- [x] Do not require `cdc.row_conflicts`, its trigger inventory procedure, or its grants to start the live stream.
- [x] Exclude retired live conflict, supersession, and automatic parent-recovery scenarios from the integration harness.

## How it works

- [Row Events](../row-events.md)
- [Target Writer](../target-writer.md)
- [Live Stream Reconnect](live-stream-reconnect.md)
- [Table Sync Repair](table-sync-repair.md)

## Implementation inventory

- `src/row/apply.rs` — maps ROW events to typed target row changes.
- `src/row/sql.rs` — builds plain INSERT, before-key UPDATE, and key DELETE SQL.
- `src/mysql_client.rs` — applies the serial INSERT-only `1062` rule.
- `src/live/parallel_target.rs` — drains parallel statements with operation metadata.
- `src/live/parallel_writer.rs` — preserves connection leasing and source-ordered commits.
- `src/live/structured_stream/transaction.rs` — owns transaction rollback and checkpoint boundaries.
- `src/live/ddl_replay_journal/` — validates only live checkpoint and DDL-journal runtime contracts.

## Tests asserting this spec

- `src/mysql_client/tests.rs`
- `src/row/tests.rs`
- `src/live/parallel_target_tests.rs`
- `src/live/structured_stream/tests/transaction.rs`
- `tests/cdc_eventual_consistency.rs`

## Known gaps (current cycle)

- [x] Prove the INSERT `1062` continuation contract against disposable real
      MariaDB/MySQL endpoints in serial and parallel modes; the harness keeps
      live source-authoritative replay separate from out-of-band conflict repair.

## Out of scope

- Deployment or production mutation.
- Target-row equality checks, merge semantics, automatic replacement, or live parent repair.
- Deleting historical conflict records or removing out-of-band repair commands.
