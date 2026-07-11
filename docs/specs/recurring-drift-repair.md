# Recurring Drift Repair

`repair-drift` orchestrates recurring bounded table reconciliation after the live
CDC stream has applied forward changes. It uses the existing `sync-table` repair
path and does not alter live row-conflict semantics.

## What it must do

- [x] Create a fresh run-scoped ID for every orchestration invocation.
- [x] Inventory source and target base tables before selecting repair work.
- [x] Compare source and target counts plus bounded content checks, invoking `sync-table` for count- or content-drifted tables.
- [x] Skip missing target tables and incompatible primary-key/column inventories with an explicit reason.
- [x] Support deterministic parent-first ordering through an explicit table-order prefix, then lexical ordering for remaining tables.
- [x] Require an explicit `--max-deletes` value in apply mode so orphan deletion is always bounded by operator input.
- [x] Preserve the existing `sync-table` primary-key repair path, including no-cross-primary-key conflict behavior.

## How it works

- [Catchup and table repair runbook](../catchup.md)
- [Table Sync Repair](table-sync-repair.md)

## Implementation inventory

- `src/repair_drift.rs` - orchestration config, inventory/count/content planning, ordering, run IDs, and command dispatch.
- `src/main.rs` - top-level command registration and CLI usage.
- `src/table_sync.rs` - bounded per-table repair execution and progress persistence.

## Tests asserting this spec

- `src/repair_drift.rs` - ordering, count/content-drift selection, content-check wiring, and apply delete-safety tests.
- `src/table_sync.rs` - primary-key repair and conflict-safe target writes.

Content checks are intentionally bounded: at most 1,000 mismatch ranges are
recorded, and floating-point columns are skipped because cross-server numeric
normalization is unsafe. Reports expose both bounds (`range_limit_exceeded`) and
skipped columns; operators must use reviewed `sync-table` columns when those
limitations matter.

## Known gaps (current cycle)

- [ ] Add integration coverage against disposable MariaDB/MySQL endpoints.

## Out of scope

- Automatic foreign-key discovery or mutation. Operators provide the parent-first prefix from the reviewed schema inventory.
- Unbounded orphan deletion.
- Live-stream automatic repair scheduling.
