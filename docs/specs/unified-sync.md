# Unified source-authoritative synchronization

The unified synchronization engine runs prerequisite schema convergence, source-authoritative row synchronization, and final constraint convergence under one immutable run identity. The production `sync` command, `sync-catalog`, `resync-stream`, and `recover-lost-binlog` use this staged engine; recovery supplies one captured source evidence set and binds the run to its authorized recovery ID. Operator usage belongs in the sync runbook.

## What it must do

### Durable stage lifecycle

- [x] Execute stages in order: `prerequisite_schema`, `rows`, then `final_constraints`.
- [x] Persist stage progress per `(run_id, stage, table_name)` with the immutable run specification.
- [x] Resume a stage only when at least one selected table is not already `complete`; replay `running`, `error`, and partially complete stage state.
- [x] Mark selected tables `running` before stage execution and `complete` only after the stage succeeds.
- [x] Stop before final constraints when row execution fails.
- [x] Preserve the primary stage error when saving error progress also fails, appending cleanup errors without reporting completion.
- [x] Reject persisted progress whose run ID, stage, table name, or immutable run specification does not match the current run.
- [x] Accept `--authorize-old-run-spec-sha256` only with an exact run ID and exactly 64 lowercase hexadecimal characters; authorization must not alter or enter the serialized run identity.
- [x] Permit an authorized migration only for additive writable columns with unchanged endpoints, settings, ordered table scope, primary keys, primary-key ordering, and retained-column ordering; require compatible current source/target schemas and no rows-stage progress for changed tables.
- [x] Lock and revalidate every exact-run progress row in one serializable transaction, update only `run_spec_json`, verify affected/current row counts, explicitly roll back every failure, never retry the transaction, and make a committed retry a no-write `already_current` result.

### Source scope and execution

- [x] Convert the selected source inventory into deterministic table definitions.
- [x] Reject an empty or duplicated selection and reject a selected child whose same-schema source parent is outside the selection.
- [x] Invoke bounded row workers between the two schema stages.
- [x] Expose the staged orchestration through one `sync` CLI. Removed progress, standalone schema, drift-check, catchup-snapshot, sync-table, and repair-drift command names are rejected as unknown commands rather than aliased.
- [x] Require exactly one immutable `--run-id` or `--run-id-prefix`; default progress persistence to `cdc.sync_runs` and support repeated `--table`, `--chunk-size`, `--parallelism`, and `--progress-table` options.
- [x] Route `sync-catalog` through one unified run with shared immutable identity and `cdc.sync_runs` progress; `--progress-table` may override this default.
- [x] Route `resync-stream` through one unified run with the fixed `resync-stream:<source_identity>` run identity and `cdc.sync_runs` progress.
- [x] Route `recover-lost-binlog` through one unified run with exact `recovery_id`, captured source evidence, exact source-table progress proof, and `cdc.sync_runs` progress.
- [ ] Prove resync/recovery source-evidence capture and complete staged execution through disposable production-shaped endpoints.

### Connection construction retry

- [x] Retry a sync connection construction only when `mysql::Error::is_connectivity_error()` classifies the failure as connectivity-related.
- [x] Bound connection construction to five attempts with exponential backoff and jitter; return the last connectivity error after exhaustion and fail immediately on permanent errors.
- [x] Preserve single-attempt row-chunk and table failure behavior after connections are constructed.
- [x] Keep authorized run-spec migration transactions single-attempt; connectivity retry applies only while constructing the required connections.
- [ ] Prove through disposable MySQL fault injection that source, locked-target, and separate progress-store constructors use the retry boundary while session initialization, progress schema operations, SQL statements, and completed stages remain single-attempt.

### Schema and progress contracts

- [x] Reuse the prerequisite schema stage that removes blocking target constraints and converges structure before row work.
- [x] Reuse the final-constraint stage after row work and fail closed on remaining structural drift.
- [ ] Prove the complete production MySQL path, including source evidence reads, target schema stages, row workers, and `cdc.sync_runs` persistence against disposable endpoints.
- [x] Remove legacy snapshot, table-sync, repair-drift, and obsolete progress modules; no fallback engine remains.

## How it works

- [Schema synchronization details](sync-schema.md) define source-to-target structural convergence within the staged `sync` run; there is no standalone schema command.
- [Lost-binlog recovery](lost-binlog-recovery.md) records the recovery caller routed through unified sync.

## Implementation inventory

- `src/main.rs` — registers the unified `sync` command and excludes obsolete command names from dispatch/help.
- `src/sync_cli.rs` — parses the unified sync endpoint, scope, runtime, progress, and immutable run-identity options.
- `src/sync/orchestrate.rs` — stage ordering, immutable progress validation, resumable stage persistence, source-scope selection, and production executor wiring.
- `src/sync/run.rs` — bounded deterministic row-table execution.
- `src/sync/chunk.rs` — locked source/target chunk mutation and progress boundary.
- `src/sync/mysql.rs` — source, locked target-session, separate progress-store adapters, and the MySQL exact-run migration transaction.
- `src/sync/progress.rs` — `cdc.sync_runs` SQL and progress serialization.
- `src/sync/run_spec_migration.rs` — additive compatibility planning, persisted-spec/hash validation, changed-table progress gates, and idempotent locked-state decisions.
- `src/sync_schema.rs` — source evidence reads plus prerequisite and final schema-stage planning/execution.
- `src/table_catalog.rs` — catalog validation and one-run `SyncConfig` mapping for `sync-catalog`.
- `src/lost_binlog_recovery.rs` — source-coordinate/evidence capture, unified-sync invocation for resync and authorized recovery, exact run/table proof, and checkpoint/barrier transition.
- `deploy.sh` — builds the fixed-base runtime image and updates only the live stream manifest; reviewed unified-sync Jobs are managed separately. The image contract is defined in [Runtime container image](runtime-image.md).

## Tests asserting this spec

- `tests/sync_cli.rs` — unified help/dispatch, obsolete-command rejection, accepted options, and obsolete-flag rejection.
- `src/main/tests/sync_cli_config.rs` — endpoint, scope, defaults, runtime options, and exclusive immutable run identity parsing.
- `src/main/tests/sync_orchestrator.rs` — stage order, resume behavior, immutable progress identity, table-selection validation, error persistence, and row-failure cutoff.
- `src/main/tests/sync_runner.rs` — bounded deterministic table execution, completion behavior, and no retry of row-chunk failures.
- `src/main/tests/sync_chunk_boundary.rs` — locked chunk ordering and checkpoint boundary.
- `src/main/tests/sync_mysql_adapter.rs` and `src/main/tests/sync_mysql_contract.rs` — adapter and SQL contracts, including bounded connectivity-only connection construction retry.
- `src/main/tests/resync_unified.rs` — resync run identity, all-table mapping, and changed-table reporting.
- `src/main/tests/lost_binlog_unified.rs` — recovery run identity, source-only proof evidence, exact progress scope, and incomplete/wrong-run rejection.
- `src/main/tests/sync_config.rs`, `src/main/tests/sync_run_spec_migration.rs`, and `src/main/tests/sync_run_spec_migration_store.rs` — authorization parsing, additive compatibility, locked-state decisions, transactional rollback/count verification, and runtime wiring.
- `scripts/cdc-integration-harness.py` scenario `sync-authorized-additive-spec-migration` — real MariaDB-to-MySQL wrong-hash no-write, atomic migration, preserved metadata, data convergence, idempotence, and changed-table row-progress rejection.

## Known gaps (current cycle)

- [ ] Migrate scripts, fixtures, grants, harnesses, and ops callers.
- [ ] Prove the complete catalog/resync/recovery MySQL paths against disposable endpoints, including connection-construction and post-connect failure boundaries.
- [x] Prove authorized additive run-spec migration against disposable MariaDB and MySQL endpoints.
- [x] Delete legacy production engines and progress paths.
- [ ] Run full-project tests, Clippy without warning suppression, and final integration verification.

## Out of scope

- Changes to live CDC transaction, duplicate-1062, checkpoint, leasing, TLS, or shutdown behavior.
- Deployment, production database mutation, registry pushes, and ops rollout.
- Compatibility aliases or fallback synchronization engines.
- General run-spec rewriting, changed-table rows-stage reinterpretation, scope/setting/key changes, or automatic migration without the exact persisted hash.
