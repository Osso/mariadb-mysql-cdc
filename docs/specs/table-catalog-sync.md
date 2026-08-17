# Table catalog sync

`table-catalog` inventories source and target tables and writes a syncable catalog;
`sync-catalog` maps that catalog into one unified sync run. Operational details belong
in [the table catalog sync wiki](../wiki/systems/table-catalog-sync.md).

## What it must do

### Catalog generation

- [x] Write deterministic, pretty JSON syncable and non-syncable catalogs without using `COUNT(*)`; preflight output paths for lexical, intermediate-symlink-plus-`..`, symlink, hardlink, and symlink-cycle conflicts. After catalog generation, open both outputs without truncation, compare opened file identities, then truncate and write through those same handles. Path changes cannot redirect the second write over the first. If both outputs were nonexistent, a failed final identity check may leave an empty created file but must not overwrite existing content. For partly nonexistent paths, canonicalize the longest existing physical ancestor before normalizing the remaining suffix.
- [x] Emit a syncable catalog as `{"tables":[{"name":"...","primary_key":["..."],"primary_key_ordering":[{"kind":"native"}|{"kind":"enum","labels":["..."]}],"columns":["..."],"estimated_source_rows":0,"parent_dependencies":["..."]}]}` and a non-syncable catalog as `{"tables":[{"name":"...","estimated_source_rows":0,"reasons":["..."]}]}`. `primary_key_ordering` is aligned one-for-one with `primary_key`; ENUM labels preserve source declaration order. Field types are strings, tagged ordering objects, string arrays, and a non-negative integer row estimate; generated syncable entries have non-empty primary-key, ordering, and column arrays, parent dependencies may be empty, and non-syncable reasons are non-empty.
- [x] Include only source base tables that exist on target, have compatible writable schemas, contain no unsupported generated columns, and have a non-empty primary key.
- [x] Require both catalog commands to receive an explicit, non-empty `--target-tls-ca-file PATH`; the catalog command contract defines no default path.
- [x] Require source and target table default character sets (derived from table collations) to match, and require each corresponding writable column's `CHARACTER_SET_NAME` and `COLLATION_NAME` to match exactly; classify any mismatch as `incompatible_schema`.
- [x] Order both catalog entry arrays by estimated source `information_schema.TABLE_ROWS`, then table name. Preserve primary-key and writable-column inventory order; emit unique parent dependencies lexicographically and reason arrays in enum declaration order.
- [x] Include ordered primary-key columns, writable sync columns, estimated source rows, and the union of applicable source and target FK parent dependencies. Evaluate FK locality against the schema owning each inventory; target-only local FKs contribute parent dependencies like source FKs, while a target FK referencing the source schema remains cross-schema.
- [x] Classify every excluded source base table with these stable snake-case reason codes: `missing_primary_key` (source has no primary key), `missing_target_table` (target table is absent), `incompatible_schema` (writable schema is not compatible), `unsupported_generated_columns` (source has a generated column), `cross_schema_dependency` (a source or target FK parent belongs to another schema), and `dependency_on_non_syncable` (a local parent dependency was excluded). A same-named local table does not satisfy a cross-schema dependency. Preserve all existing reasons when adding `dependency_on_non_syncable`, and propagate it transitively to every affected descendant.

### Catalog execution

- [x] `sync-catalog` reads the supplied syncable JSON and invokes one unified `sync` run, blocking until the staged operation completes or fails; `table-catalog` only writes catalogs and does not start sync or dump work.
- [x] Map every catalog table into one unified `SyncConfig` with the catalog source/target, ordered table names, configured chunk size, bounded catalog parallelism, `cdc.sync_runs` by default (overridable with `--progress-table`), and shared `--run-id-prefix` identity.
- [x] Persist one immutable run identity and staged progress in `cdc.sync_runs`; schema prerequisites, locked source-authoritative row chunks, and final constraints are owned by unified sync.
- [x] Do not run the removed per-table catalog scheduler, admission locks, child run IDs, dependency gating, target-only repair verification, or per-table progress handling. The unified prerequisite schema stage removes blocking target constraints before row execution; unified bounded row workers then execute the selected scope.
- [x] Read catalog JSON without mutating it and never execute full dumps; the non-syncable catalog is classification/operator input only.
- [x] Regenerate syncable catalogs after any change to primary-key ordering semantics. Catalog metadata remains validated before it is mapped into unified sync tables.

## How it works

- [Table catalog sync](../wiki/systems/table-catalog-sync.md)
- [Table sync repair](table-sync-repair.md)

## Implementation inventory

- `src/table_catalog.rs` — catalog models, inventory classification, JSON I/O, CLI parsing, and one-run unified configuration mapping.
- `src/sync/orchestrate.rs` — unified schema/row/constraint execution and durable `cdc.sync_runs` progress.
- `src/main.rs` — command dispatch and usage text.

## Tests asserting this spec

- `src/main/tests/sync_catalog_unified.rs` — one unified configuration with all catalog tables, shared run identity, progress table, chunk size, and bounded parallelism.
- `src/table_catalog.rs` — deterministic classification, FK locality and propagation, physical output alias/cycle rejection, catalog validation, and JSON ordering.
- `src/inventory/tests/` — referenced-parent schema query, parsing, and inventory preservation tests.

## Known gaps (current cycle)

- [ ] Prove the complete catalog-to-unified MySQL path against disposable endpoints.
- [ ] Migrate the lost-binlog recovery caller; `resync-stream` now uses unified sync.
- [ ] Remove legacy snapshot, table-sync, repair-drift, and progress modules after all callers migrate.

## Out of scope

- Full-dump execution; the non-syncable catalog is an operator input only and is never consumed by `sync-catalog`.
- Deployment or automatically starting `sync-catalog` after `table-catalog` generation.
