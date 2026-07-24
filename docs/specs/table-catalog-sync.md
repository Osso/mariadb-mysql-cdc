# Table catalog sync

`table-catalog` inventories source and target tables for resumable table sync, while
`sync-catalog` applies the generated syncable catalog through the existing table-sync
engine. Operational details belong in [the table catalog sync wiki](../wiki/systems/table-catalog-sync.md).

## What it must do

### Catalog generation

- [x] Write deterministic, pretty JSON syncable and non-syncable catalogs without using `COUNT(*)`; preflight output paths for lexical, intermediate-symlink-plus-`..`, symlink, hardlink, and symlink-cycle conflicts. After catalog generation, open both outputs without truncation, compare opened file identities, then truncate and write through those same handles. Path changes cannot redirect the second write over the first. If both outputs were nonexistent, a failed final identity check may leave an empty created file but must not overwrite existing content. For partly nonexistent paths, canonicalize the longest existing physical ancestor before normalizing the remaining suffix.
- [x] Emit a syncable catalog as `{"tables":[{"name":"...","primary_key":["..."],"columns":["..."],"estimated_source_rows":0,"parent_dependencies":["..."]}]}` and a non-syncable catalog as `{"tables":[{"name":"...","estimated_source_rows":0,"reasons":["..."]}]}`. Field types are strings, string arrays, and a non-negative integer row estimate; generated syncable entries have non-empty primary-key and column arrays, parent dependencies may be empty, and non-syncable reasons are non-empty.
- [x] Include only source base tables that exist on target, have compatible writable schemas, contain no unsupported generated columns, and have a non-empty primary key.
- [x] Require both catalog commands to receive an explicit, non-empty `--target-tls-ca-file PATH`; the catalog command contract defines no default path.
- [x] Require source and target table default character sets (derived from table collations) to match, and require each corresponding writable column's `CHARACTER_SET_NAME` and `COLLATION_NAME` to match exactly; classify any mismatch as `incompatible_schema`.
- [x] Order both catalog entry arrays by estimated source `information_schema.TABLE_ROWS`, then table name. Preserve primary-key and writable-column inventory order; emit unique parent dependencies lexicographically and reason arrays in enum declaration order.
- [x] Include ordered primary-key columns, writable sync columns, estimated source rows, and the union of applicable source and target FK parent dependencies. Evaluate FK locality against the schema owning each inventory; target-only local FKs gate child scheduling exactly like source FKs, while a target FK referencing the source schema remains cross-schema.
- [x] Classify every excluded source base table with these stable snake-case reason codes: `missing_primary_key` (source has no primary key), `missing_target_table` (target table is absent), `incompatible_schema` (writable schema is not compatible), `unsupported_generated_columns` (source has a generated column), `cross_schema_dependency` (a source or target FK parent belongs to another schema), and `dependency_on_non_syncable` (a local parent dependency was excluded). A same-named local table does not satisfy a cross-schema dependency. Preserve all existing reasons when adding `dependency_on_non_syncable`, and propagate it transitively to every affected descendant.

### Catalog execution

- [x] `sync-catalog` reads the supplied syncable JSON and immediately starts apply-mode table syncs, blocking until all entries complete or a failure is returned; it has no dry-run/plan mode. `table-catalog` only writes catalogs and does not start either syncs or full dumps.
- [x] Apply catalog tables through the existing table-sync engine. Target-only rows are reconciled in dependency-safe chunks; each chunk is applied, exactly verified, and durably checkpointed before the next chunk.
- [x] Require a non-empty `--run-id-prefix` identifying each immutable catalog attempt. Interrupted children resume only with the same prefix and immutable specification; a retry attempt must use a fresh prefix rather than reusing the prior attempt's child identities.
- [x] Use deterministic fixed-length `catalog-v2-<SHA-256 hex>` run IDs. Hash the injective length-framed byte tuple `(prefix, target database, table)` and keep every generated ID within the 128-byte progress-column limit while preserving prefix/database/table identity.
- [x] Limit table-sync capacity to four target-server slots shared by direct `sync-table` and `sync-catalog` workers. New admission and slot reservations are serialized and scoped by the lower-cased target host plus port, so databases on the same server share capacity. Each worker reserves one slot plus a database/table-specific lock whose identifier components are injectively length-framed and hexadecimal-encoded. Legacy run-lock accounting and same-table detection inspect only the configured progress table. Within that table, identity comes from the immutable run specification's `scope.target_database` and `table.name`, not `table_name` alone, so same-named tables in different target databases neither block nor complete each other's catalog dependencies. A held legacy run-ID advisory lock for the requested same database/table excludes that table even without a table reservation. Rows in `running`, `complete`, or `error` with a held legacy run-ID advisory lock but no table reservation consume equivalent capacity. Ignore stale unlocked `running` and `error` rows before parsing immutable specifications. Malformed lock-active rows fail closed. Always parse an expected completed child and require its immutable specification to match exactly before treating it as terminal.
- [x] Keep reservation connections eligible to hold admission locks through long table runs by setting their MySQL session `wait_timeout` to 86,400 seconds; do not treat this as recovery from arbitrary network disconnects.
- [x] Schedule dependency-ready tables by catalog order, which is smallest estimated row count then name.
- [x] During catalog generation, exclude children of non-syncable parents with `dependency_on_non_syncable`; during execution, gate each child on every listed non-self FK parent completing. A failed parent blocks its descendants; missing dependencies are rejected before workers start, while cyclic dependencies fail closed after owned workers settle without waiting for unrelated external syncs.
- [x] Resume interrupted exact run IDs; a `status='complete'` row is terminal only when its stored immutable run specification exactly matches the current catalog child. If the expected run ID has a different stored specification, fail closed instead of treating it as terminal.
- [x] Read catalog JSON without mutating it and never execute full dumps; the non-syncable catalog is classification/operator input only.

## How it works

- [Table catalog sync](../wiki/systems/table-catalog-sync.md)
- [Table sync repair](table-sync-repair.md)

## Implementation inventory

- `src/table_catalog.rs` — catalog models, inventory classification, CLI parsing, active-run accounting, and dependency scheduler.
- `src/main.rs` — command dispatch and usage text.

## Tests asserting this spec

- `src/table_catalog.rs` — deterministic classification, cross-database status isolation, source/target FK locality and propagation, physical output alias/cycle rejection, catalog graph prevalidation, injective concurrency reservations, reservation timeout setup, stale-run handling, ordering, failure blocking, and run-ID tests.
- `src/inventory/tests/` — referenced-parent schema query, parsing, and inventory preservation tests.

## Known gaps (current cycle)

None.

## Out of scope

- Full-dump execution; the non-syncable catalog is an operator input only and is never consumed by `sync-catalog`.
- Deployment or automatically starting `sync-catalog` after `table-catalog` generation.
- More than four simultaneous table syncs, including externally active runs.
