# Table catalog sync

`table-catalog` inventories source and target schemas and writes operator-readable JSON. `sync-catalog` consumes the syncable catalog and coordinates existing `sync-table` workers. The normative contract is [the table catalog sync spec](../../specs/table-catalog-sync.md).

## Catalog generation

`table-catalog` reads schema metadata plus source `information_schema.TABLES.TABLE_ROWS` estimates. It writes:

- a PK-backed syncable catalog ordered by estimated rows, then table name;
- a non-syncable catalog with stable exclusion reasons for full-dump planning.

Classification compares writable columns, generated-column support, character sets, collations, and the union of applicable source and target FK dependencies. Target-only local FKs gate scheduling; source or target cross-schema FKs exclude the child and propagate dependency exclusions. Catalog generation rejects output paths resolving to the same filesystem destination—including lexical aliases, symlinks, and hardlinks—before writing either file. It is otherwise read-only and does not start repair or dump work.

## Scheduling

`sync-catalog` applies catalog entries with the existing resumable table-sync engine. It schedules the smallest dependency-ready table first and starts a child only after its catalog parents complete. Missing or cyclic owned dependencies fail once owned workers have settled; unrelated external work does not postpone that failure.

Direct `sync-table` and catalog workers share four target-server slots. Admission is serialized, and each worker holds one server slot plus a database/table reservation. Reservation, admission, and slot lock preimages use separate internal domains from legacy run-ID locks. Existing run-ID lock hashing remains unchanged so active and legacy runs remain detectable.

Each child uses a versioned run ID that length-frames and hexadecimal-encodes every byte of the `(prefix, target database, table)` tuple, applies with `max_deletes=0`, and persists progress in the configured run table. The final encoded run ID must fit the 128-byte progress column. Framing every component prevents delimiter ambiguity and cross-database collisions. An interrupted child resumes only with the same immutable specification. An expected completed child is parsed and terminal only when its specification matches exactly. Stale unlocked running/error rows are ignored before spec parsing; malformed lock-active rows fail closed.

## Failure and recovery

A worker failure is recorded through normal table-sync progress handling. Descendants remain blocked and the catalog command reports the causal table. Successful siblings remain complete. Re-running the same catalog prefix resumes interrupted children without mutating the catalog JSON.

The non-syncable catalog is operator input only. Full-dump execution and automatic deployment are outside this command.
