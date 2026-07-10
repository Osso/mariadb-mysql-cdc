# Row Conflict Handling

The structured CDC stream applies MariaDB ROW binlog events to a MySQL-compatible target while allowing checksum-driven reconciliation to restore eventual consistency after target conflicts.

## What it must do

- [x] Apply each source `WriteRowsEvent` row as an independent plain `INSERT` containing the explicit source primary key.
- [x] Never generate `ON DUPLICATE KEY UPDATE` for a source row insert.
- [x] Under `ignore-duplicate`, skip only the row whose target insert or update reports MySQL error 1062 and continue applying later rows from the event.
- [x] Emit a parseable `cdc_row_conflict_skipped` event containing operation, schema, table, source coordinate, and source primary key.
- [x] Preserve fail-fast behavior for duplicate row changes under the default conflict policy.
- [x] Keep generated target columns out of row insert and update statements.

## How it works

- [Statement events](../statement-events.md)
- [Table sync repair](table-sync-repair.md)

## Implementation inventory

- `src/row.rs` — constructs and applies explicit-primary-key row statements and logs skipped conflicts.
- `src/target.rs` — exposes row execution outcomes without changing generic target writers.
- `src/mysql_client.rs` — classifies duplicate row-change errors under the configured policy.
- `src/live/insert_conflict.rs` — defines duplicate-conflict policy checks.

## Tests asserting this spec

- `src/row.rs` — independent inserts, continued application after one ignored conflict, generated-column exclusion, and conflict log format.
- `src/live/insert_conflict.rs` — duplicate insert/update classification and default fail-fast policy.

## Known gaps (current cycle)

- [ ] Persist skipped conflict counters/ranges for scheduling targeted reconciliation.
- [ ] Add recurring checksum/sync orchestration that proves skipped rows converge.

## Out of scope

- Resolving unique-key swaps or cycles inside the live stream. Reconciliation owns those conflicts.
- Replaying source statement-based DML. Production source replication uses `ROW` with `FULL` row images.
