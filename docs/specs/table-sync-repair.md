# Table Sync Repair

`sync-table` reconciles one MariaDB source table against one MySQL target table in
primary-key chunks. It is the drift-repair path for catchup gaps after the live
CDC stream has already been applying forward changes.

## What it must do

- [x] Compare source and target rows by the configured primary-key columns.
- [x] Report target inserts needed for source rows missing from the target.
- [x] Report target updates needed for rows whose compared column values differ.
- [x] Report extra target rows and, in `apply` mode only, delete at most the explicit
  `--max-deletes` allowance; the default allowance is zero.
- [x] Support `dry-run` mode that reports repairs without applying them.
- [x] Support `apply` mode that inserts missing rows, updates divergent rows, and performs
  only the allowed bounded orphan deletes.
- [x] Read target rows through the source chunk end key so repair work stays
  bounded to one source chunk window.
- [x] Allow extra target rows inside a source window to be detected.
- [x] Require `--run-id` for every `sync-table` invocation.
- [x] Store resumable run-scoped progress in the target CDC database table, defaulting to
  `cdc.table_sync_runs`, and auto-create that schema/table when missing.
- [x] Allow a run ID to resume only the exact interrupted run: its immutable specification
  includes source/target endpoints and databases, target write policy, table name,
  primary-key and selected-column shape, mode, chunk size, range bounds, maximum deletes,
  and the optional `updated-since` accelerator.
- [x] Reject concurrent processes using the same run ID with a target-side named lock.
- [x] Reject reuse of a completed run ID; recurring or changed repair work requires a new
  run ID.
- [x] Keep legacy `cdc.table_sync_progress` for snapshot catchup only; it is not a
  `sync-table` run store.
- [x] Make interrupted `--updated-since` retries restart from the beginning under the same
  run ID, preventing newly eligible rows behind a saved primary key from being skipped;
  upsert matching source rows without deleting target orphans.
- [ ] On stream target-apply failure for INSERT, UPDATE, or REPLACE, schedule a
  bounded table repair for the affected table/window and checkpoint the event
  only after repair succeeds.
- [ ] Do not checkpoint a failed DELETE through repair until bounded target
  deletes are supported by the live repair path.

## How it works

- [Catchup and table repair runbook](../catchup.md)

## Implementation inventory

- `src/table_sync.rs` - chunk comparison, repair reporting, MySQL row readers,
  and target repair adapter.
- `src/table_sync/progress.rs` - legacy catchup progress schema plus run-scoped
  table-sync progress schema, loading, saving, and error recording.
- `src/sync_cli.rs` - `sync-table` option parsing and command dispatch.
- `src/main.rs` - top-level command registration and shared option helpers.
- `src/live/insert_conflict.rs` - observable live conflict classification; automatic
  repair scheduling remains a known gap.

## Tests asserting this spec

- `src/table_sync.rs` - row comparison, dry-run/apply behavior, source-window
  target reads, and SQL generation tests.
- `src/table_sync/progress.rs` - legacy and run-scoped progress-table DDL, upsert SQL,
  and load parsing tests.
- `src/sync_cli.rs` - `sync-table` parser and required run-ID tests.
- `src/live/insert_conflict.rs` and `src/row.rs` - secondary-unique conflict safety
  and conflict log tests.

## Known gaps (current cycle)

- [ ] Automate multi-table scheduling from schema inventory.
- [ ] Add row-level divergence output suitable for operator review.
- [ ] Scope stream repairs to affected primary-key windows when the failed SQL
  allows safe key extraction.

## Out of scope

- Unbounded deletion is out of scope. Orphan deletion requires `apply` mode and an
  explicit nonzero `--max-deletes` allowance.
- Automatic cutover is out of scope. This command repairs rehearsal target drift.
