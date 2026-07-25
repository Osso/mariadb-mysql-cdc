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
- [x] Maintain actual streamed-DDL parity: a mapping accepted by `sync-schema` must produce the same translated MySQL semantics as the corresponding streamed DDL operation.
- [x] Have no alternate mapping, compatibility fallback, direct source-DDL execution path, or silent approximation.
- [x] Fail explicitly on unsupported or ambiguous source constructs.

### Convergence and destructive changes

- [x] Apply by default; there is no implicit dry-run mode.
- [x] Allow destructive convergence, including dropping target-only selected-table objects, changing column definitions, and narrowing types when the actual-data preflight proves the conversion safe.
- [x] Preflight existing target data before every potentially lossy column conversion.
- [x] Never truncate, coerce, discard, clamp, or silently rewrite existing target values to make an ALTER succeed.
- [x] If data would be rejected, truncated, or coerced, fail that table's operation, report the blocking condition and representative primary-key sample values, and continue independent tables.
- [x] Apply foreign keys and other dependency-sensitive objects only after their prerequisites converge; skip dependent operations when a prerequisite failed.

### Failure and verification

- [x] Continue independent table operations after a statement or table failure; dependency-blocked operations are reported as skipped rather than attempted.
- [x] Re-inventory every selected table after its operations finish.
- [x] Report remaining semantic differences after re-inventory.
- [x] Emit structured JSON for every table, statement, preflight, skip, error, and final verification outcome, including source/target fingerprints and representative blocker keys.
- [x] Exit nonzero when any selected table is divergent, blocked, skipped due to a failed prerequisite, or otherwise not converged.
- [x] Do not persist a schema journal or claim transactional rollback for MySQL DDL's implicit commits.

## How it works

- [DDL transformation contract](ddl-transformation.md)
- [Schema inventory](../schema-inventory.md)

## Implementation inventory

- `src/sync_schema.rs` — selection, schema planning, target-data preflight, sequential execution, re-inventory, structured reporting, and CLI parsing.
- `src/live/ddl_semantics.rs` and `src/live/ddl_semantics/transform.rs` — sole shared MariaDB-to-MySQL DDL translation contract used by streamed replay and schema convergence.
- `src/main.rs` — `sync-schema` command dispatch.

## Tests asserting this spec

- `src/sync_schema.rs` covers selection, exact convergence planning, destructive preflight with blocker counts/sample keys, safe conversions, dependency ordering/skips, best-effort continuation, re-inventory status, JSON/exit behavior, and translator parity.
- `src/live/ddl_semantics/tests.rs` covers shared translation acceptance, parity, and rejection without a generic fallback.

## Known gaps

- [ ] Add containerized MariaDB/MySQL end-to-end coverage before production use.

## Out of scope

- Dropping target tables that were not selected.
- Row-data synchronization, repair, or deletion of target rows to satisfy schema convergence.
- A persistent schema journal, resume protocol, or rollback abstraction over implicitly committed DDL.
- A second or fallback MariaDB-to-MySQL mapping implementation.
