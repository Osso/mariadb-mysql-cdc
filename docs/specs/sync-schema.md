# Schema synchronization

`sync-schema` converges explicitly selected target tables to a MySQL 8-compatible semantic translation of the authoritative source MariaDB schema. It is a schema operation, not a row-data synchronization command.

## What it must do

### Selection and authority

- [x] Accept repeated `--table TABLE` arguments, a catalog input, or `--all-tables true` defining the selected tables. `--all-tables true` selects every source base table read from the source inventory, and is rejected when combined with `--table` or `--catalog`.
- [x] Treat the source schema as authoritative only within the selected-table set.
- [x] Converge each selected table exactly: target-only columns, indexes, foreign keys, checks, and generated expressions are removed; unselected target tables are never dropped.
- [x] Apply selected tables sequentially, one table at a time.

### Shared DDL translation

- [x] Reuse the sole shared MariaDB-to-MySQL 8 DDL translator used by streamed DDL replay.
- [x] Generate schema operations through that translator and apply the same semantic mappings, including temporal types, defaults, `ON UPDATE`, generated expressions, character sets, collations, indexes, checks, and foreign keys.
- [x] Emit column `CHARACTER SET` and `COLLATE` as part of the data type, before nullability, defaults, and generated expressions.
- [x] Converge the target to the source schema plus the unique parent indexes MySQL requires and MariaDB does not: when a source foreign key's referenced columns are not the leftmost prefix of a source primary key or unique index, expect a synthesized `uq_cdc_<parent>_<columns>` unique index on the parent. Create it when absent, keep it when present, and require it before adding the dependent foreign key.
- [x] Translate `ENUM` columns, preserving the case of their values; `SET` columns are still rejected explicitly.
- [x] Render a literal default as the source already spells it, quoting only a bare value, so a MariaDB string or bit literal is never quoted twice.
- [x] Name check constraints so they are unique per schema as MySQL requires: a source name used by more than one table is qualified with its table, and a name already unique keeps its source spelling.
- [x] Reference a foreign-key parent in the converged schema without a database qualifier so it resolves in the target database; only a genuinely cross-schema parent keeps its qualifier.
- [x] Maintain actual streamed-DDL parity: a mapping accepted by `sync-schema` must produce the same translated MySQL semantics as the corresponding streamed DDL operation.
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
- [x] Compare a source `TIMESTAMP` column against a target `DATETIME` column as converged, while a target still holding `TIMESTAMP` remains divergent.
- [x] Express a foreign key's parent schema relative to the endpoint reporting it, so a target database whose name differs from the source still compares equal for a same-schema parent.
- [x] Read the source check-constraint inventory from the table-scoped MariaDB view rather than joining every same-named constraint to every table.

### Convergence and destructive changes

- [x] Apply by default; there is no implicit dry-run mode.
- [x] Allow destructive convergence, including dropping target-only selected-table objects, changing column definitions, and narrowing types when the actual-data preflight proves the conversion safe.
- [x] Preflight existing target data before every potentially lossy column conversion.
- [x] Never truncate, coerce, discard, clamp, or silently rewrite existing target values to make an ALTER succeed.
- [x] If data would be rejected, truncated, or coerced, fail that table's operation, report the blocking condition and representative primary-key sample values, and continue independent tables.
- [x] Apply foreign keys and other dependency-sensitive objects only after their prerequisites converge; skip dependent operations when a prerequisite failed.

### Failure and verification

- [x] Continue independent table operations after a statement or table failure; dependency-blocked operations are reported as skipped rather than attempted.
- [x] Re-inventory every selected table after its operations finish, reading only that table's metadata and fetching it in one round-trip.
- [x] Plan nothing on a second run against a converged target.
- [x] Log each table's position, status, and statement counts to stderr as it completes, so a long run is distinguishable from a hang. The JSON report stays the sole stdout output.
- [x] Report remaining semantic differences after re-inventory.
- [x] Emit structured JSON for every table, statement, preflight, skip, error, and final verification outcome, including source/target fingerprints and representative blocker keys.
- [x] Exit nonzero when any selected table is divergent, blocked, skipped due to a failed prerequisite, or otherwise not converged.
- [x] Do not persist a schema journal or claim transactional rollback for MySQL DDL's implicit commits.

## How it works

- [DDL transformation contract](ddl-transformation.md)
- [Schema inventory](../schema-inventory.md)

## Implementation inventory

- `src/sync_schema.rs` — selection, schema planning, canonical MariaDB/MySQL metadata comparison, target-data preflight, sequential execution, re-inventory, structured reporting, and CLI parsing.
- `src/inventory/reader.rs` and `src/inventory/query.rs` — optional table scope and the batched single-round-trip scoped read.
- `src/live/ddl_semantics.rs` and `src/live/ddl_semantics/transform.rs` — sole shared MariaDB-to-MySQL DDL translation contract used by streamed replay and schema convergence.
- `src/main.rs` — `sync-schema` command dispatch.

## Tests asserting this spec

- `src/sync_schema.rs` covers `--all-tables` selection, the canonical column comparison against every measured MariaDB/MySQL metadata disagreement, literal-default rendering, `ENUM` value case, synthesized parent unique indexes, relative parent schemas, check-name qualification and order-independent comparison, selection, exact convergence planning, destructive preflight with blocker counts/sample keys, safe conversions, dependency ordering/skips, best-effort continuation, re-inventory status, JSON/exit behavior, and translator parity.
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
- Row-data synchronization, repair, or deletion of target rows to satisfy schema convergence.
- A persistent schema journal, resume protocol, or rollback abstraction over implicitly committed DDL.
- A second or fallback MariaDB-to-MySQL mapping implementation.
