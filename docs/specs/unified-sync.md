# Unified source-authoritative synchronization

The unified synchronization engine runs prerequisite schema convergence,
source-authoritative row synchronization, and final constraint convergence under
one durable progress run ID. The production `sync` command, `sync-catalog`,
`resync-stream`, and `recover-lost-binlog` use this staged engine; recovery
supplies one captured source evidence set and uses its authorized recovery ID as
the run ID. Operator usage belongs in the sync runbook.

## What it must do

### Durable stage lifecycle

- [x] Execute stages in order: `prerequisite_schema`, `rows`, then `final_constraints`.
- [x] Persist progress per `(run_id, stage, table_name)`; within the selected progress store, the run ID is the only durable invocation identity.
- [x] Load and validate only the stored run ID, stage, and table name. Do not load or compare persisted invocation configuration.
- [x] Leave omitted-table rows untouched, skip selected rows already marked `complete`, replay selected `running` or `error` rows, and create missing rows for newly selected tables.
- [x] Resume row progress from its stored primary-key cursor and counters while applying the current chunk size and table definition.
- [x] Mark only incomplete selected tables `running` before schema-stage execution and `complete` only after that stage succeeds.
- [x] Stop before final constraints when row execution fails.
- [x] Preserve the primary stage error when saving error progress also fails, appending cleanup errors without reporting completion.
- [x] Retain the physical `run_spec_json` column as ignored legacy schema: select queries omit it, new rows insert `{}`, and duplicate-key updates never rewrite it.
- [x] Require no run-spec authorization, comparison, migration, or rewriting.

### Source scope and execution

- [x] Convert the selected current source inventory into deterministic table definitions, including writable `BIT`, `ENUM`, and `MEDIUMBLOB` runtime metadata while omitting runtime metadata from backward-compatible `sync-v1` ID serialization.
- [x] Reject an empty or duplicated selection and reject a selected child whose same-schema source parent is outside the current selection.
- [x] Invoke bounded row workers between the two schema stages.
- [x] Expose the staged orchestration through one `sync` CLI. Removed progress, standalone schema, drift-check, catchup-snapshot, sync-table, and repair-drift command names are rejected as unknown commands rather than aliased.
- [x] Require exactly one `--run-id` or `--run-id-prefix`; default progress persistence to `cdc.sync_runs` and support repeated `--table`, `--chunk-size`, `--parallelism`, and `--progress-table` options.
- [x] Preserve an exact `--run-id` unchanged. Preserve backward-compatible `sync-v1` prefix-derived IDs from the prefix plus serialized invocation/table input; this private derivation input is not persisted or compared as progress identity.
- [x] Treat `--run-id-prefix` as backward-compatible ID generation only, not the recommended mutable-resume path: changing its serialized input changes the derived ID. Use an exact `--run-id` to resume the same progress across invocation changes.
- [x] Resolve source and target configuration, current table scope and definitions, chunk size, and parallelism fresh on every invocation without authorization or durable-state migration. The selected progress table locates the store; changing it selects a different store and does not cross-read prior rows.
- [x] Route `sync-catalog` through one unified run with one prefix-derived durable run ID and `cdc.sync_runs` progress; `--progress-table` may override this default.
- [x] Route `resync-stream` through one unified run with the fixed `resync-stream:<source_identity>` run ID and `cdc.sync_runs` progress.
- [x] Route `recover-lost-binlog` through one unified run with exact `recovery_id`, captured source evidence, exact source-table progress proof, and `cdc.sync_runs` progress.
- [ ] Prove resync/recovery source-evidence capture and complete staged execution through disposable production-shaped endpoints.

### Strict row mutations and secondary-unique repair

- [x] Keep unified-sync row mutations source-authoritative and strict: normal missing-row work uses plain batched `INSERT`; never use `INSERT IGNORE`, upsert, `REPLACE`, or a fallback engine.
- [x] Reconcile each bounded source window through target keyset pages without retaining the complete target window: delete each page's target-only keys in the locked transaction, retain only source-bounded divergent/missing rows, then apply updates and inserts after all target-only deletes succeed.
- [x] On a strict insert `1062`, reconcile only the named full-column, non-`PRIMARY` secondary unique index reported by MySQL. The target session keeps the existing table `WRITE` lock and transaction, resolves exactly one owner per intended row with NULL-safe `<=>` predicates, and exact-reads that owner primary key from the current source.
- [x] Require contiguous metadata for every full indexed column. Prefixed columns, expression columns, `PRIMARY`, absent or ambiguous index metadata, absent or ambiguous owner evidence, and NULL-valued unique identities fail closed.
- [x] Reconcile a different-primary-key owner to its complete current source row, or delete it when that source primary key is absent. Fail closed when the current source owner still owns the intended unique identity or its primary key or column set disagrees.
- [x] Verify each owner mutation and intended source row, then retry only the failed insert batch plus untouched remaining insert rows. Repeated conflict keys fail rather than retry indefinitely; any repair, retry, verification, or commit failure rolls back the locked chunk and leaves durable progress unchanged.
- [x] Commit the target chunk before persisting progress. Emit secret-free reconciliation audits only after successful commit; discard pending audits on rollback or commit failure. Keep counters tied to planned source operations without changing live CDC duplicate handling.
- [x] Round-trip every writable `BIT` column as an unsigned integer for source/target reads and target mutation bindings, preserving `NULL` and values through `BIT(64)` for strict inserts and CASE updates.
- [x] Round-trip every writable `ENUM` column by internal index and bind it as an unsigned integer, preserving index zero, declared empty labels, numeric labels, and `NULL`; enum primary-key cursors retain declaration labels.
- [x] Round-trip every writable `MEDIUMBLOB` column through lossless hexadecimal read projection and strict byte bindings, preserving `NULL`, empty values, and invalid UTF-8.

### Connection construction retry

- [x] Retry sync connection construction only when `mysql::Error::is_connectivity_error()` classifies the failure as connectivity-related.
- [x] Bound connection construction to five attempts with exponential backoff and jitter; return the last connectivity error after exhaustion and fail immediately on permanent errors.
- [x] Preserve single-attempt row-chunk and table failure behavior after connections are constructed.
- [ ] Prove through disposable MySQL fault injection that source, locked-target, and separate progress-store constructors use the retry boundary while session initialization, progress schema operations, SQL statements, and completed stages remain single-attempt.

### Schema and progress contracts

- [x] Reuse the prerequisite schema stage that removes blocking target constraints and converges structure before row work.
- [x] Reuse the final-constraint stage after row work and fail closed on remaining structural drift.
- [ ] Prove the complete production MySQL path, including source evidence reads, target schema stages, row workers, and `cdc.sync_runs` persistence against disposable endpoints.
- [x] Remove legacy snapshot, table-sync, repair-drift, run-spec migration, and obsolete progress modules; no fallback engine remains.

## How it works

- [Schema synchronization details](sync-schema.md) define source-to-target structural convergence within the staged `sync` run; there is no standalone schema command.
- [Lost-binlog recovery](lost-binlog-recovery.md) records the recovery caller routed through unified sync.

## Implementation inventory

- `src/main.rs` — registers the unified `sync` command and excludes obsolete command names and authorization flags from help and dispatch.
- `src/sync_cli.rs` — parses current endpoints, scope, runtime, progress location, and run-ID options.
- `src/sync/config.rs` — validates the current invocation and resolves exact or prefix-derived run IDs.
- `src/sync/orchestrate.rs` — stage ordering, run/stage/table progress validation, resumable stage persistence, source-scope selection, and production executor wiring.
- `src/sync/run.rs` — bounded deterministic row-table execution.
- `src/sync/chunk.rs` — locked source/target chunk mutation and run/table progress boundary.
- `src/sync/mysql.rs` — source, locked target-session, separate progress-store adapters, and enum primary-key cursor reconstruction.
- `src/sync/sql.rs` — metadata-aware projections and strict bindings for `BIT`, `ENUM`, and `MEDIUMBLOB` columns.
- `src/sync/progress.rs` — `cdc.sync_runs` SQL plus legacy-column-neutral progress serialization.
- `src/sync_schema.rs` — source evidence reads plus prerequisite and final schema-stage planning/execution.
- `src/table_catalog.rs` — catalog validation and one-run `SyncConfig` mapping for `sync-catalog`.
- `src/lost_binlog_recovery.rs` — source-coordinate/evidence capture, unified-sync invocation for resync and authorized recovery, exact run/table proof, and checkpoint/barrier transition.
- `deploy.sh` — builds the fixed-base runtime image and updates only the live stream manifest; reviewed unified-sync Jobs are managed separately. The image contract is defined in [Runtime container image](runtime-image.md).

## Tests asserting this spec

- `tests/sync_cli.rs` — unified help/dispatch, obsolete-command rejection, accepted options, and obsolete authorization-flag rejection.
- `src/main/tests/sync_cli_config.rs` — endpoint, scope, defaults, runtime options, and exclusive run-ID parsing.
- `src/main/tests/sync_config.rs` — exact-ID preservation, runtime metadata-neutral `sync-v1` derivation, source-inventory conversion, and audited `ENUM`/`MEDIUMBLOB` SQL and binding fidelity.
- `src/main/tests/sync_orchestrator.rs` — stage order, changed-invocation resume, omitted/new/complete table behavior, run/stage/table validation, error persistence, and row-failure cutoff.
- `src/main/tests/sync_runner.rs` — bounded deterministic table execution, current parallelism/scope validation, completion behavior, and no retry of row-chunk failures.
- `src/main/tests/sync_chunk_boundary.rs` — locked chunk ordering, bounded target-page reconciliation, changed-chunk-size resume, checkpoint boundary, strict secondary-unique owner repair, rollback, retry, verification, and post-commit audit behavior.
- `src/main/tests/sync_mysql_adapter.rs` and `src/main/tests/sync_mysql_contract.rs` — adapter and SQL contracts, including numeric `BIT` projection/binding, ignored legacy run specs, strict insert batching, exact owner/index reads, fail-closed index handling, and bounded connectivity-only connection construction retry.
- `scripts/cdc-integration-harness.py` — disposable MariaDB-to-MySQL `sync-bit-values` proof for unchanged `BIT(1)` zero updates, flips, `BIT(9)`, `BIT(64)`, `NULL`, inserts, deletes, and pagination.
- `src/main/tests/resync_unified.rs` — resync run ID, all-table mapping, and changed-table reporting.
- `src/main/tests/lost_binlog_unified.rs` — recovery run ID, source-only proof evidence, exact progress scope, and incomplete/wrong-run rejection.
- `scripts/cdc-integration-harness.py` scenario `sync-unique-owner-rollback-resume`, with `tests/cdc_eventual_consistency.rs` wrapper — real MariaDB-to-MySQL strict insert rollback/resume and progress-boundary proof.
- `scripts/cdc-integration-harness.py` scenario `sync-resume`, with `tests/cdc_eventual_consistency.rs` wrapper — disposable same-run resume through a changed target address from parallelism 1 to 16 without restarting committed row progress.

## Known gaps (current cycle)

- [x] Prove disposable MariaDB-to-MySQL same-run resume across changed target address and parallelism without restarting completed work through the `sync-resume` scenario.
- [ ] Prove the complete catalog/resync/recovery MySQL paths against disposable endpoints, including connection-construction and post-connect failure boundaries.
- [x] Delete legacy production engines, run-spec migration, and obsolete progress paths.
- [ ] Run full-project tests, Clippy without warning suppression, and final integration verification.

## Out of scope

- Changes to live CDC transaction, duplicate-1062, checkpoint, leasing, TLS, or shutdown behavior.
- Deployment, production database mutation, registry pushes, and ops rollout.
- Compatibility aliases, fallback synchronization engines, or rewriting existing `run_spec_json` values.
