# Table catalog sync

`table-catalog` inventories source and target tables for resumable table sync, while
`sync-catalog` applies the generated syncable catalog through the existing table-sync
engine. Operational details belong in [the table catalog sync wiki](../wiki/systems/table-catalog-sync.md).

## What it must do

### Catalog generation

- [x] Write deterministic, pretty JSON syncable and non-syncable catalogs without using `COUNT(*)`.
- [x] Include only source base tables that exist on target, have compatible writable schemas, contain no unsupported generated columns, and have a non-empty primary key.
- [x] Order syncable entries by estimated source `information_schema.TABLE_ROWS`, then table name.
- [x] Include ordered primary-key columns, writable sync columns, estimated source rows, and FK parent dependencies.
- [x] Classify every excluded source base table with stable reason codes and propagate `dependency_on_non_syncable` to children.

### Catalog execution

- [x] Apply catalog tables through the existing table-sync engine with `max_deletes=0` and deterministic run IDs derived from a required prefix.
- [x] Limit total active table syncs to four, counting externally lock-active runs in `cdc.table_sync_runs` and never duplicating an active table.
- [x] Schedule dependency-ready tables by catalog order, which is smallest estimated row count then name.
- [x] Start children only after all catalog parents complete; explicitly fail when a parent fails or dependencies cannot resolve.
- [x] Resume interrupted exact run IDs and treat completed exact run IDs as terminal.
- [x] Read catalog JSON without mutating it and never execute full dumps.

## How it works

- [Table catalog sync](../wiki/systems/table-catalog-sync.md)
- [Table sync repair](table-sync-repair.md)

## Implementation inventory

- `src/table_catalog.rs` — catalog models, inventory classification, CLI parsing, active-run accounting, and dependency scheduler.
- `src/main.rs` — command dispatch and usage text.

## Tests asserting this spec

- `src/table_catalog.rs` — deterministic classification, dependency propagation, concurrency reservation, stale-run handling, ordering, failure blocking, and run-ID tests.

## Known gaps (current cycle)

None.

## Out of scope

- Full-dump execution; the non-syncable catalog is an operator input only.
- Deployment or automatically starting a catalog sync after catalog generation.
- More than four simultaneous table syncs.
