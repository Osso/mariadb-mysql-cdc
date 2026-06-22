# Table Sync Repair

`sync-table` reconciles one MariaDB source table against one MySQL target table in
primary-key chunks. It is the drift-repair path for catchup gaps after the live
CDC stream has already been applying forward changes.

## What it must do

- [x] Compare source and target rows by the configured primary-key columns.
- [x] Report target inserts needed for source rows missing from the target.
- [x] Report target updates needed for rows whose compared column values differ.
- [x] Report extra target rows without deleting them.
- [x] Support `dry-run` mode that reports repairs without applying them.
- [x] Support `apply` mode that inserts missing rows and updates divergent rows.
- [x] Read target rows through the source chunk end key so repair work stays
  bounded to one source chunk window.
- [x] Allow extra target rows inside a source window to be detected.
- [x] Parse source, target, table, column, chunk-size, and mode options from the
  `sync-table` CLI command.

## How it works

- [Table sync repair wiki](../wiki/systems/table-sync-repair.md)

## Implementation inventory

- `src/table_sync.rs` - chunk comparison, repair reporting, MySQL row readers,
  and target repair adapter.
- `src/sync_cli.rs` - `sync-table` option parsing and command dispatch.
- `src/main.rs` - top-level command registration and shared option helpers.

## Tests asserting this spec

- `src/table_sync.rs` - row comparison, dry-run/apply behavior, source-window
  target reads, and SQL generation tests.
- `src/sync_cli.rs` - `sync-table` parser tests.

## Known gaps (current cycle)

- [ ] Automate multi-table scheduling from schema inventory.
- [ ] Persist per-table sync progress for long repair runs.
- [ ] Add row-level divergence output suitable for operator review.

## Out of scope

- Deletes are out of scope for this first repair command; extra target rows are
  reported so an operator can review them before destructive action.
- Automatic cutover is out of scope. This command repairs rehearsal target drift.
