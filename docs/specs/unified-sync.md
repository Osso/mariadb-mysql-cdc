# Unified source-authoritative synchronization

The unified synchronization engine runs prerequisite schema convergence, source-authoritative row synchronization, and final constraint convergence under one immutable run identity. The production `sync` command and migration of existing callers remain in progress; operator usage belongs in the eventual sync runbook.

## What it must do

### Durable stage lifecycle

- [x] Execute stages in order: `prerequisite_schema`, `rows`, then `final_constraints`.
- [x] Persist stage progress per `(run_id, stage, table_name)` with the immutable run specification.
- [x] Resume a stage only when at least one selected table is not already `complete`; replay `running`, `error`, and partially complete stage state.
- [x] Mark selected tables `running` before stage execution and `complete` only after the stage succeeds.
- [x] Stop before final constraints when row execution fails.
- [x] Preserve the primary stage error when saving error progress also fails, appending cleanup errors without reporting completion.
- [x] Reject persisted progress whose run ID, stage, table name, or immutable run specification does not match the current run.

### Source scope and execution

- [x] Convert the selected source inventory into deterministic sync-table definitions.
- [x] Reject an empty or duplicated selection and reject a selected child whose same-schema source parent is outside the selection.
- [x] Invoke bounded row workers between the two schema stages.
- [ ] Wire the orchestration to the single `sync` CLI and remove the obsolete command names rather than aliasing them.
- [ ] Route lost-binlog recovery, resync, catalog, and other callers through unified sync.

### Schema and progress contracts

- [x] Reuse the prerequisite schema stage that removes blocking target constraints and converges structure before row work.
- [x] Reuse the final-constraint stage after row work and fail closed on remaining structural drift.
- [ ] Prove the complete production MySQL path, including source evidence reads, target schema stages, row workers, and `cdc.sync_runs` persistence against disposable endpoints.
- [ ] Replace legacy snapshot, table-sync, repair-drift, and progress modules after all callers migrate.

## How it works

- [Schema synchronization](sync-schema.md) defines source-to-target structural convergence.
- [Table sync repair](table-sync-repair.md) records the legacy row-repair contract being replaced by this engine.
- [Lost-binlog recovery](lost-binlog-recovery.md) records the recovery caller that must migrate to unified sync.

## Implementation inventory

- `src/sync/orchestrate.rs` — stage ordering, immutable progress validation, resumable stage persistence, source-scope selection, and production executor wiring.
- `src/sync/run.rs` — bounded deterministic row-table execution.
- `src/sync/chunk.rs` — locked source/target chunk mutation and progress boundary.
- `src/sync/mysql.rs` — source, locked target-session, and separate progress-store adapters.
- `src/sync/progress.rs` — `cdc.sync_runs` SQL and progress serialization.
- `src/sync_schema.rs` — source evidence reads plus prerequisite and final schema-stage planning/execution.

## Tests asserting this spec

- `src/main/tests/sync_orchestrator.rs` — stage order, resume behavior, immutable progress identity, table-selection validation, error persistence, and row-failure cutoff.
- `src/main/tests/sync_runner.rs` — bounded deterministic table execution and completion behavior.
- `src/main/tests/sync_chunk_boundary.rs` — locked chunk ordering and checkpoint boundary.
- `src/main/tests/sync_mysql_adapter.rs` and `src/main/tests/sync_mysql_contract.rs` — adapter and SQL contracts.

## Known gaps (current cycle)

- [ ] Add the unified CLI parser, help, dispatch, and obsolete-command rejection.
- [ ] Migrate recovery, resync, catalog, scripts, fixtures, grants, harnesses, and ops callers.
- [ ] Delete legacy production engines and progress paths.
- [ ] Run full-project tests, Clippy without warning suppression, and final integration verification.

## Out of scope

- Changes to live CDC transaction, duplicate-1062, checkpoint, leasing, TLS, or shutdown behavior.
- Deployment, production database mutation, registry pushes, and ops rollout.
- Compatibility aliases or fallback synchronization engines.
