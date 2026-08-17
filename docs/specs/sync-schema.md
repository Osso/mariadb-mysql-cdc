# Staged schema synchronization

The prerequisite and final-constraint stages of `sync` converge selected target
tables to a MySQL 8-compatible semantic translation of the authoritative source
MariaDB schema. Schema convergence is part of the staged run, not a standalone
command or separate row-data operation.

## What it must do

### Selection and authority

- [x] Derive selected tables from the unified `sync` configuration and source inventory.
- [x] Treat the source schema as authoritative only within the selected-table set.
- [x] Converge each selected table exactly: target-only columns, indexes, foreign keys, checks, and generated expressions are removed. Tables outside the selected source set are not dropped.
- [x] Apply prerequisite and final-constraint stages in durable run order.

### Shared DDL translation

- [x] Reuse the sole shared MariaDB-to-MySQL 8 DDL translator used by streamed DDL replay.
- [x] Generate schema operations through that translator and apply the same semantic mappings, including temporal types, defaults, `ON UPDATE`, generated expressions, character sets, collations, indexes, checks, and foreign keys.
- [x] Emit column `CHARACTER SET` and `COLLATE` as part of the data type, before nullability, defaults, and generated expressions.
- [x] Converge the target to the source schema plus the unique parent indexes MySQL requires and MariaDB does not: when a source foreign key's referenced columns are not the leftmost prefix of a source primary key or unique index, expect a synthesized `uq_cdc_<parent>_<columns>` unique index on the parent. Create it when absent, keep it when present, and require it before adding the dependent foreign key.
- [x] Translate `ENUM` columns, preserving the case of their values; `SET` columns are still rejected explicitly.
- [x] Render a literal default as the source already spells it, quoting only a bare value, so a MariaDB string or bit literal is never quoted twice.
- [x] Render every target `CHECK` constraint name as table-qualified for MySQL: prepend the owning table and an underscore unless the source name is already qualified with that table, so an already-qualified name is not doubled. If the rendered name exceeds MySQL's 64-character identifier limit, shorten it deterministically with a collision-resistant digest derived from the constraint kind, table, and canonical source name.
- [x] Render every target `FOREIGN KEY` constraint name with its child table identity under the same no-double-qualification and deterministic collision-resistant shortening rules. The same source name on different child tables therefore remains distinct.
- [x] Preserve canonical source constraint identity and source fingerprints as evidence; planning, target introspection, drift comparison, and final verification compare the rendered target constraint identity instead of raw source names.
- [x] Reference a foreign-key parent in the converged schema without a database qualifier so it resolves in the target database; only a genuinely cross-schema parent keeps its qualifier.
- [x] Maintain actual streamed-DDL parity: a mapping accepted by a staged `sync` schema phase must produce the same translated MySQL semantics as the corresponding streamed DDL operation.
- [x] Have no alternate mapping, compatibility fallback, direct source-DDL execution path, or silent approximation.
- [x] Fail explicitly on unsupported or ambiguous source constructs.

### Comparing MariaDB metadata with MySQL metadata

Both engines describe an identical converged column differently, so comparison uses one canonical form. Without it every column reads as divergent, every run re-issues the same `ALTER`, and no table can verify as converged.

- [x] Treat an integer display width as absent, because MySQL 8 does not store one.
- [x] Read a MariaDB literal default as its value: quotes removed, and the literal `NULL` treated as no default.
- [x] Treat `current_timestamp()` and `CURRENT_TIMESTAMP` as one default, and preserve the case of every other default value because it is data.
- [x] Ignore MySQL's `DEFAULT_GENERATED` extra marker.
- [x] Map MariaDB's UCA-1400 collations to their MySQL equivalents in both comparison and generated DDL.
- [x] Compare a stored expression - a generated column or a check clause - ignoring the parentheses, charset introducers, and spacing MySQL adds when it re-renders one.
- [x] Carry `TIMESTAMP` across unchanged. It is not mapped to `DATETIME`: MySQL stores every value this source holds, so a source `TIMESTAMP` column converges only against a target `TIMESTAMP` column.
- [x] Express a foreign key's parent schema relative to the endpoint reporting it, so a target database whose name differs from the source still compares equal for a same-schema parent.
- [x] Read the source check-constraint inventory from the table-scoped MariaDB view rather than joining every same-named constraint to every table.

### Convergence and destructive changes

- [x] Apply by default; there is no implicit dry-run mode.
- [x] Allow destructive convergence, including dropping target-only selected-table objects, changing column definitions, and narrowing types when the actual-data preflight proves the conversion safe.
- [x] Preflight existing target data before every potentially lossy column conversion.
- [x] Skip the preflight for a conversion no target row can violate, rather than counting every row
      as a blocker because no predicate is expressible. MariaDB spells a JSON column `LONGTEXT` with
      a `json_valid()` CHECK while MySQL has a native type, and every stored JSON document
      serialises into `LONGTEXT`, so replacing the native type with that spelling can neither reject
      nor truncate a row. The reverse direction is not safe and still preflights.
- [x] Never truncate, coerce, discard, clamp, or silently rewrite existing target values to make an ALTER succeed.
- [x] If data would be rejected, truncated, or coerced, fail that table's operation, report the blocking condition and representative primary-key sample values, and continue independent tables.
- [x] Apply foreign keys and other dependency-sensitive objects only after their prerequisites converge; skip dependent operations when a prerequisite failed.
- [x] Recreate CHECK constraints only after referenced columns converge and all planned same-table foreign-key drops complete; a failed foreign-key drop blocks CHECK re-add.

### Failure and verification

- [x] Continue independent table operations after a statement or table failure; dependency-blocked operations are reported as skipped rather than attempted.
- [x] Execute planned schema statements and fail closed on statement or prerequisite errors.
- [x] Persist stage status in `cdc.sync_runs`; stage completion follows successful execution, not a separate verification command.
- [x] Log schema-stage failures with table and statement context.
- [x] Do not persist a schema journal or claim transactional rollback for MySQL DDL's implicit commits.

## How it works

- [DDL transformation contract](ddl-transformation.md)
- [Schema inventory](../schema-inventory.md)

## Implementation inventory

- `src/sync_schema.rs` — source-evidence reads, schema planning, canonical MariaDB/MySQL metadata comparison, target-data preflight, statement execution, and reusable prerequisite/final-constraint stages for unified sync.
- `src/inventory/reader.rs` and `src/inventory/query.rs` — optional table scope and the batched single-round-trip scoped read.
- `src/live/ddl_semantics.rs` and `src/live/ddl_semantics/transform.rs` — sole shared MariaDB-to-MySQL DDL translation contract used by streamed replay and schema convergence.
- `src/sync/orchestrate.rs` — durable invocation of prerequisite and final-constraint schema stages.

## Tests asserting this spec

- `src/sync_schema.rs` covers source-evidence planning, canonical column comparison against every measured MariaDB/MySQL metadata disagreement, literal-default rendering, `ENUM` value case, synthesized parent unique indexes, relative parent schemas, table-qualified CHECK/FOREIGN KEY target names, deterministic bounded shortening, rendered-identity comparison, exact convergence planning, destructive preflight with blocker counts/sample keys, safe conversions, dependency ordering/skips, failure handling, and translator parity.
- `src/live/ddl_semantics/tests.rs` covers shared translation acceptance including `ENUM`, parity, and rejection without a generic fallback.
- `src/inventory/tests/parsing.rs` covers the table-scoped queries and the distinction between an empty-string default and no default.

## Known gaps

- [ ] Add containerized MariaDB/MySQL end-to-end coverage before production use.
- [ ] Each converging change is a separate statement, so a table needing several column changes is rebuilt once per change. Batching a table's clauses into one `ALTER TABLE` would rebuild it once.

## Rehearsal evidence (2026-07-25)

Against a schema-only copy of the live do-managed `globalcomix` target (452 tables, verified byte-identical in column and table metadata before the run) with the prod MariaDB source as authority:

- First run: 462 selected tables, 1043 statements, all executed, **462 converged**, no remaining differences, exit 0, 187 s.
- Second run: **0 statements planned**, no differences, exit 0.

Applying this to the live target is a heavy operation, not a metadata change: 820 `TIMESTAMP` columns across 384 tables must become `DATETIME`, MySQL rejects `ALGORITHM=INPLACE` for a column type change, and the affected tables hold about 864 GB of data and indexes. Each such `ALTER` rebuilds the table with `ALGORITHM=COPY` and blocks writes to it, including the CDC stream's.

## Out of scope

- Dropping target tables that were not selected.
- Row-data mutation; unified `sync` owns that staged operation separately.
- A persistent schema journal, resume protocol, or rollback abstraction over implicitly committed DDL.
- A second or fallback MariaDB-to-MySQL mapping implementation.
