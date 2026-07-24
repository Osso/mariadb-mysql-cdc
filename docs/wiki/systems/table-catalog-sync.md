# Table catalog sync

`table-catalog` inventories source and target schemas and writes operator-readable JSON. `sync-catalog` consumes the syncable catalog and coordinates existing `sync-table` workers. The normative contract is [the table catalog sync spec](../../specs/table-catalog-sync.md).

## Catalog generation

`table-catalog` reads schema metadata plus source `information_schema.TABLES.TABLE_ROWS` estimates. It writes:

- a PK-backed syncable catalog ordered by estimated rows, then table name;
- a non-syncable catalog with stable exclusion reasons for full-dump planning.

Classification compares writable columns, generated-column support, character sets, collations, and the union of applicable source and target FK dependencies. FK locality is evaluated against the schema owning each inventory; a target FK referencing the source schema remains cross-schema. Target-only local FKs gate scheduling; source or target cross-schema FKs exclude the child and propagate dependency exclusions. A same-named local table does not make a cross-schema FK local. Catalog generation preflights output paths for lexical aliases, intermediate-symlink-plus-`..` aliases, symlinks, hardlinks, and symlink cycles. After catalog content is generated, it opens both outputs without truncation, compares the opened file identities, and only then truncates and writes through those same handles. Path changes cannot redirect the second write over the first. A failed final identity check may leave an empty file when a destination did not previously exist, but does not overwrite existing content. It canonicalizes the longest existing physical ancestor before lexically resolving a nonexistent suffix. It is otherwise read-only and does not start repair or dump work.

## Scheduling

`sync-catalog` applies catalog entries with the existing resumable table-sync engine. It schedules the smallest dependency-ready table first and starts a child only after its catalog parents complete. Missing dependencies are rejected before workers start. Cyclic owned dependencies fail once owned workers have settled; unrelated external work does not postpone that failure.

Direct `sync-table` and catalog workers share four target-server slots. Admission is serialized, and each worker holds one server slot plus a database/table reservation. Database and table reservation components are length-framed and hexadecimal-encoded so delimiter-bearing identifiers remain distinct. Reservation, admission, and slot lock preimages use separate internal domains from legacy run-ID locks. Existing run-ID lock hashing remains unchanged so active and legacy runs remain detectable.

Each `sync-catalog` invocation requires an explicit bounded `--max-deletes` value and a non-empty `--run-id-prefix`. Every child uses a fixed-length `catalog-v2-<SHA-256 hex>` run ID derived from the injective length-framed byte tuple `(prefix, target database, table)`, applies with that bound, and persists progress in the configured run table. The child's immutable run specification records `max_deletes`; a violating target-orphan chunk fails before mutation. Hashing preserves tuple identity while keeping long valid names within the 128-byte progress column. An interrupted child resumes only with the same prefix and immutable specification. A retry attempt uses a fresh prefix, producing fresh child identities rather than reusing the prior attempt. An expected completed child is parsed and terminal only when its specification matches exactly. Stale unlocked running/error rows are ignored before spec parsing; malformed lock-active rows fail closed.

## Failure and recovery

A worker failure is recorded through normal table-sync progress handling. Descendants remain blocked and the catalog command reports the causal table. Successful siblings remain complete. Re-running the same catalog prefix resumes interrupted children without mutating the catalog JSON; a new retry attempt must use a fresh prefix and explicitly choose its bounded delete allowance.

The non-syncable catalog is operator input only. Full-dump execution and automatic deployment are outside this command.
