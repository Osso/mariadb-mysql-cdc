# Table catalog sync

`table-catalog` inventories source and target schemas and writes operator-readable JSON. `sync-catalog` consumes the syncable catalog and coordinates existing `sync-table` workers. The normative contract is [the table catalog sync spec](../../specs/table-catalog-sync.md).

## Catalog generation

`table-catalog` reads schema metadata plus source `information_schema.TABLES.TABLE_ROWS` estimates. It writes:

- a PK-backed syncable catalog ordered by estimated rows, then table name;
- a non-syncable catalog with stable exclusion reasons for full-dump planning.

Classification compares writable columns, generated-column support, character sets, collations, and local FK dependencies. Catalog generation rejects identical syncable and non-syncable output paths before writing either file. It is otherwise read-only and does not start repair or dump work.

## Scheduling

`sync-catalog` applies catalog entries with the existing resumable table-sync engine. It schedules the smallest dependency-ready table first and starts a child only after its catalog parents complete. Missing or cyclic owned dependencies fail once owned workers have settled; unrelated external work does not postpone that failure.

Direct `sync-table` and catalog workers share four target-server slots. Admission is serialized, and each worker holds one server slot plus a database/table reservation. Reservation, admission, and slot lock preimages use separate internal domains from legacy run-ID locks. Existing run-ID lock hashing remains unchanged so active and legacy runs remain detectable.

Each child uses `<run-id-prefix>-<normalized-target-database>-<table>`, applies with `max_deletes=0`, and persists progress in the configured run table. The database component preserves only ASCII letters and digits and encodes every other UTF-8 byte as `_xx`, including `_` and `-`. For example, `a b` becomes `a_20b`, while literal `a_20b` becomes `a_5f20b`. The final encoded run ID must fit the 128-byte progress column. This prevents the same prefix/table pair from colliding across target databases. An interrupted child resumes only with the same immutable specification. An expected completed child is parsed and terminal only when its specification matches exactly. Stale unlocked running/error rows are ignored before spec parsing; malformed lock-active rows fail closed.

## Failure and recovery

A worker failure is recorded through normal table-sync progress handling. Descendants remain blocked and the catalog command reports the causal table. Successful siblings remain complete. Re-running the same catalog prefix resumes interrupted children without mutating the catalog JSON.

The non-syncable catalog is operator input only. Full-dump execution and automatic deployment are outside this command.
