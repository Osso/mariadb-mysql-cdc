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

- [x] Convert the selected current source inventory into deterministic table definitions, including writable `BIT`, `ENUM`, and `MEDIUMBLOB` runtime metadata. Omit new `ENUM` and `MEDIUMBLOB` metadata from backward-compatible `sync-v1` ID serialization; retain existing `BIT` serialization.
- [x] Reject an empty or duplicated selection and reject a selected child whose same-schema source parent is outside the current selection.
- [x] Invoke bounded row workers between the two schema stages. For tables without completed legacy `rows` progress, run source-authoritative `insert_missing`, `update_divergent`, then `delete_extras` phases; schedule inserts and updates parent-first and deletes child-first from current same-schema FK inventory. Only unrelated tables share a bounded batch. Before a worker reads source rows, it locks its complete selected same-schema FK component for `WRITE` through mutation, commit, and phase-progress persistence.
- [x] Apply `--parallelism` to independent schema tables as well as row workers. Preserve statement order within each table, finish all constraint drops before additions, wait for selected parent tables, and block dependents of failed parents. Reject unresolved dependency cycles and retain deterministic report order. Schema workers use independent target sessions; this does not enable hot-reloading an already-running recovery.
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
- [x] Bound retained projected-row payload to 64 MiB per source/target read page as well as the requested row limit. Preserve a single larger row intact; this budget is not a universal process-memory or row-size limit.
- [x] Distinguish a byte-limited partial page from exhausted data. Continue from the last retained primary key without skipping rows, and do not complete target-tail cleanup while more rows remain. A full requested SQL row limit also reports continuation.
- [x] Reconcile each bounded source window through target keyset pages without retaining the complete target window: delete each page's target-only keys in the locked transaction, retain only source-bounded divergent/missing rows, then apply updates and inserts after all target-only deletes succeed.
- [x] On a strict insert or update `1062`, reconcile only the named full-column, non-`PRIMARY` secondary unique index reported by MySQL. The target session keeps the existing table `WRITE` lock and transaction, resolves exactly one different-primary-key owner per conflicting intended row with NULL-safe `<=>` predicates (ignoring already-correct same-primary-key ownership within an update batch), and exact-reads that owner primary key from the current source.
- [x] Require contiguous metadata for every full indexed column. Prefixed columns, expression columns, `PRIMARY`, absent or ambiguous index metadata, absent or ambiguous owner evidence, and NULL-valued unique identities fail closed.
- [x] Reconcile a different-primary-key owner to its complete current source row, or delete it when that source primary key is absent. Fail closed when the current source owner still owns the intended unique identity or its primary key or column set disagrees.
- [x] Verify each owner mutation and intended source row, then retry only the failed mutation batch plus untouched remaining rows of that mutation kind. Repeated conflict keys fail rather than retry indefinitely; any repair, retry, verification, or commit failure rolls back the locked chunk and leaves durable progress unchanged.
- [x] Commit the target chunk before persisting progress. Emit secret-free reconciliation audits only after successful commit; discard pending audits on rollback or commit failure. Keep counters tied to planned source operations without changing live CDC duplicate handling.
- [x] On a restrictive referenced-key parent update rejected with MySQL `1217` or `1451`, keep constraints enabled and, inside the component `WRITE` lock and the same target transaction, detach the affected target-dependent subtree child-first, update the root parent, and restore only current source rows parent-first. Strict writes read back every detached/restored row; source-absent dependent rows remain deleted. This transition never drops or disables a foreign key.
- [ ] Prove the restrictive referenced-key transition against disposable MariaDB/MySQL. Current implementation bounds retained work at 1,000,000 keys; fails when a desired dependent references a parent outside the detached source-authoritative work set; and does not yet handle a new child insert that references a parent whose referenced key changes in the same run.
- [x] Round-trip every writable `BIT` column as an unsigned integer for source/target reads and target mutation bindings, preserving `NULL` and values through `BIT(64)` for strict inserts and CASE updates.
- [x] Round-trip every writable `ENUM` column by internal index and bind it as an unsigned integer, preserving index zero, declared empty labels, numeric labels, and `NULL`; enum primary-key cursors retain declaration labels. An index-zero ENUM primary key fails explicitly because it has no unambiguous label cursor.
- [x] Round-trip every writable `MEDIUMBLOB` column through lossless hexadecimal read projection and strict byte bindings, preserving `NULL`, empty values, and invalid UTF-8.

**Datatype audit status.** A disposable MariaDB-to-MySQL audit passed representative fixtures for all 19 datatype families in the observed source inventory at revision `0e1fd4e`. The prior baseline passed 17 families and failed `ENUM` ordinal distinction and `MEDIUMBLOB` byte fidelity. The passing audit exercises strict insert/update, target-only deletion, NULL and empty distinctions, and multiple pages. It does not prove every possible value, index-zero ENUM primary keys, or production recovery completion.

### Connection construction retry

- [x] Retry sync connection construction only when `mysql::Error::is_connectivity_error()` classifies the failure as connectivity-related.
- [x] Bound connection construction to five attempts with exponential backoff and jitter; return the last connectivity error after exhaustion and fail immediately on permanent errors.
- [x] Preserve single-attempt row-chunk and table failure behavior after connections are constructed.
- [ ] Prove through disposable MySQL fault injection that source, locked-target, and separate progress-store constructors use the retry boundary while session initialization, progress schema operations, SQL statements, and completed stages remain single-attempt.

### Schema and progress contracts

- [x] Preserve existing target foreign keys and CHECK constraints when prerequisite structural convergence is empty. This avoids routine constraint rebuilds for unchanged tables.
- [x] For actual prerequisite structural changes, retain the existing constraint-drop behavior; this intermediate planner change does not claim selective structural dependency analysis.
- [x] Automatically converge only source ENUM declarations that append labels after every existing target label in unchanged order, with unchanged charset, equivalent mapped collation, generated expression, and compatible nullability. Preserve existing ordinals; reject reordered or removed labels rather than broadening conversion.
- [x] Reuse the final-constraint stage after row work to apply semantic foreign-key definition differences and fail closed on remaining structural drift. Equal FK semantics retain an existing target name; a necessary drop uses that actual target name, while a new constraint uses the mapped target name.
- [x] Keep independent phase cursors in an additive target table named `<progress-table>_phases`, keyed by unchanged run ID, table name, and explicit mutation phase. A completed legacy `rows` record is reused unchanged; incomplete legacy cursors do not seed phase cursors. The aggregate legacy `rows` record is written only after all three phases finish.
- [ ] Prove crash/retry behavior across target commit and phase-cursor persistence. The phase cursor uses a separate target connection and is not yet atomically committed with row mutations.
- [ ] Prove the integrated phase executor against disposable MariaDB/MySQL endpoints: valid existing FK/CHECK preservation; parent/child insert, reparent, and delete ordering; restart after each phase; phase/legacy progress consistency; semantic FK-name preservation; and no `ALTER TABLE` for unchanged schemas. The real restrictive-transition harness reached final-constraint planning and exposed the semantic FK-name defect fixed in `d2ce16f`; rerun remains pending. Phase cursor persistence currently uses a separate post-commit target connection, so crash/retry behavior must be proven rather than described as atomic. No deployment has occurred.

**Known constraint-preserving limits.** Parent-key updates protected by `RESTRICT`, cross-table replacement transitions, FK cycles/self-references, and phase-specific secondary-unique-owner reconciliation are not solved by phased scheduling. They must fail closed or receive separately proven coordinated behavior; they do not authorize routine FK drops.
- [x] Remove legacy snapshot, table-sync, repair-drift, run-spec migration, and obsolete progress modules; no fallback engine remains.

## How it works

- [Schema synchronization details](sync-schema.md) define source-to-target structural convergence within the staged `sync` run; there is no standalone schema command.
- [Lost-binlog recovery](lost-binlog-recovery.md) records the recovery caller routed through unified sync.

## Implementation inventory

- `src/main.rs` — registers the unified `sync` command and excludes obsolete command names and authorization flags from help and dispatch.
- `src/sync_cli.rs` — parses current endpoints, scope, runtime, progress location, and run-ID options.
- `src/sync/config.rs` — validates the current invocation and resolves exact or prefix-derived run IDs.
- `src/sync/orchestrate.rs` — stage ordering, run/stage/table progress validation, resumable stage persistence, source-scope selection, and production executor wiring.
- `src/sync/run.rs` — bounded deterministic row-table execution helper.
- `src/sync/dependency_order.rs` and `src/sync/phased_run.rs` — deterministic FK dependency batches and integrated parent-first/child-first phase execution.
- `src/sync/phase_progress.rs` — additive explicit phase cursor storage; it does not alter legacy `cdc.sync_runs` rows.
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
- [ ] Run full-project tests, Clippy without warning suppression, and final integration verification. The September 9, 2026 phased-row implementation has only focused unit coverage; no disposable end-to-end phase proof or deployment exists.

## Out of scope

- Changes to live CDC transaction, duplicate-1062, checkpoint, leasing, TLS, or shutdown behavior.
- Deployment, production database mutation, registry pushes, and ops rollout.
- Compatibility aliases, fallback synchronization engines, or rewriting existing `run_spec_json` values.
