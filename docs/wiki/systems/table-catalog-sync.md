# Table catalog sync

`table-catalog` inventories source and target schemas and writes operator-readable JSON. `sync-catalog` consumes the syncable catalog and invokes one unified sync run. The normative contract is [the table catalog sync spec](../../specs/table-catalog-sync.md).

## Catalog generation

`table-catalog` reads schema metadata plus source `information_schema.TABLES.TABLE_ROWS` estimates. It writes:

- a PK-backed syncable catalog ordered by estimated rows, then table name;
- a non-syncable catalog with stable exclusion reasons for full-dump planning.

Classification compares writable columns, generated-column support, character sets, collations, and the union of applicable source and target FK dependencies. FK locality is evaluated against the schema owning each inventory; a target FK referencing the source schema remains cross-schema. Target-only local FKs contribute parent dependencies; source or target cross-schema FKs exclude the child and propagate dependency exclusions. A same-named local table does not make a cross-schema FK local. Catalog generation preflights output paths for lexical aliases, intermediate-symlink-plus-`..` aliases, symlinks, hardlinks, and symlink cycles. After catalog content is generated, it opens both outputs without truncation, compares the opened file identities, and only then truncates and writes through those same handles. Path changes cannot redirect the second write over the first. A failed final identity check may leave an empty file when a destination did not previously exist, but does not overwrite existing content. It canonicalizes the longest existing physical ancestor before lexically resolving a nonexistent suffix. It is otherwise read-only and does not start repair or dump work.

## Unified execution

`sync-catalog` maps every catalog entry into one unified `SyncConfig` and invokes one run. The run uses the configured source and target, ordered table names, chunk size, bounded catalog parallelism, progress table, and shared non-empty `--run-id-prefix`. Unified sync derives one immutable run identity and persists staged progress in `cdc.sync_runs`.

The unified run owns prerequisite schema convergence, locked source-authoritative row chunks, bounded row workers, and final constraint convergence. The removed catalog-specific dependency scheduler, admission locks, child run IDs, target-only repair verification, and per-table progress handling are not part of this path. Catalog FK metadata still controls which tables are classified as syncable; it does not create separate child runs.

## Failure and recovery

A unified run failure is recorded through `cdc.sync_runs` and returned by `sync-catalog`. Resume behavior follows the unified run identity and staged progress contract; the catalog JSON is not mutated. Recovery and resync callers remain separate migration work.

The non-syncable catalog is operator input only. Full-dump execution and automatic deployment are outside this command.
