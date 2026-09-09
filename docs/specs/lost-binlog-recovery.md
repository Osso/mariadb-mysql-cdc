# Lost-binlog recovery

`recover-lost-binlog` is the audited availability-first transition for a stream whose durable MariaDB binlog checkpoint names purged history. It consumes an operator JSON authorization, captures one non-locking MariaDB binlog coordinate plus one committed source evidence set, then runs the unified staged sync for the exact source-table scope before atomically advancing only the authorized stream checkpoint and superseding only the exact historical journal barrier. `resume-lost-binlog` resumes that full prepared transition; `activate-lost-binlog` may instead commit an already completed prepared recovery while final FK convergence remains deferred. Binlog events after the captured coordinate remain eligible for replay after recovery. The implementation is present on this branch; deployment, production execution, restart health, and post-transition verification are not claimed here.

## What it must do

### Authorization and scope

- [x] Read an authorization JSON containing the exact old checkpoint, exact journal barrier and SQL, source identity, checkpoint name, recovery ID, operator identity, and reason.
- [x] Reject a configured source identity or checkpoint name that does not match the authorization.
- [x] Compute and record the current source scope hash for each attempt; when authorization supplies a scope hash, reject an authorization hash that differs from that attempt's current source scope.
- [x] Reject any configured source table whose engine is not InnoDB.

### Anchored reconciliation

- [x] Acquire the same `cdc-stream:{target.database}` lease used by live streaming before recovery state changes.
- [x] Capture the MariaDB binlog coordinate with ordinary non-locking source reads; source recovery must not require `FLUSH TABLES WITH READ LOCK`, `UNLOCK TABLES`, `LOCK TABLES`, or `RELOAD`.
- [x] Reconcile normally committed source rows and schema evidence without a long-lived cross-table transaction or repeatable-read snapshot.
- [x] Invoke one unified staged sync for every source-table in the captured inventory, using run ID `recovery_id`, configured `--parallelism` (default `1`) for independent schema-table and row workers, and the configured `cdc.sync_runs` progress table plus its additive `cdc.sync_runs_phases` cursor table. Per-table statement order and selected-parent dependencies remain ordered. A running recovery is not hot-reloaded after an image update.
- [x] Preserve the replay boundary: source commits after the captured coordinate remain eligible for stream binlog replay after recovery advances the checkpoint.
- [x] Under `replace-divergent-pk`, acknowledge the known `1644` external-payment trigger only for services `8` and `9` when the target row is unchanged, exactly one existing target external-payment owner has the incoming numeric primary key, the complete target row equals the current source row including owner and order, and the current source external identity equals the incoming identity. Otherwise preserve the original error and roll back atomically. Keep the trigger active; do not blanket-ignore `1644`. Diagnostics expose numeric primary keys and boolean checks only.
- [x] Use the unified stages for constraint-preserving prerequisite schema convergence, source-authoritative chunks locked across their selected FK component before source reads, durable phase and aggregate per-table progress, and final constraint convergence. A same-ID resume reuses completed legacy `rows` progress unchanged without requiring phase-table access.
- [x] Set only recovery-owned source/target coordinator and sync-progress sessions to `SESSION wait_timeout=604800` before long reconciliation. This matches the seven-day recovery Job deadline; it does not change server-global timeouts, normal stream/sync sessions, CLI configuration, or reconnect behavior.
- [x] Keep prepared evidence source-only: scope hash, source schema fingerprint, and source table count; no target inventory is captured for preparation proof.
- [x] Require every expected source table to have exactly one complete progress result for the exact recovery ID; missing, unexpected, duplicate, incomplete, or differently identified rows fail closed.
- [x] Recheck the captured source scope hash before checkpoint transition; a changed source scope blocks proof.
- [x] Refuse checkpoint transition when unified stage execution or exact run/table progress proof fails.
- [x] Prove the complete CLI path against disposable MariaDB/MySQL endpoints: bootstrap, exact authorization refusal, full current scope with a non-PK generated column, committed recovery, preserved historical barrier, and post-transition restart/row replay. Production execution is not claimed.

### Durable transition

- [x] Insert an immutable `prepared` recovery record containing old state, new coordinate, source identity, scope, operator, reason, and evidence.
- [x] Resume only an existing `prepared` record with the same recovery ID, operator, reason, old checkpoint, barrier, source identity, scope, and immutable prepared evidence. Omitted authorization scope/evidence bind to the stored values; supplied values must match.
- [x] Resume reuses the original run ID, captured coordinate, and durable table progress without a new boundary capture, replacement ID, abandonment, checkpoint reset, or full-rescan fallback. Each invocation may select `--parallelism` independently.
- [x] Before reconciliation and again immediately before checkpoint commit, require the original captured file and position to remain present in `SHOW BINARY LOGS`; a missing or shorter file fails without checkpoint/progress mutation.
- [x] Before reconstructing source evidence, require the current complete InnoDB inventory hash and both stored prepared-evidence hashes to match the immutable prepared scope.
- [x] Lock the checkpoint, exact barrier, new recovery ID, and exact-barrier recovery owner in one preparation transaction.
- [x] When a separately authorized recovery ID replaces a `prepared` owner for the exact checkpoint, barrier, and source identity, atomically mark only the old row `abandoned` with server-generated `abandoned_at` and evidence binding both recovery IDs, operator, reason, checkpoint, barrier, source identity, and both attempts' scopes, then insert the replacement `prepared` row.
- [x] Preserve all old identity, scope, and prepared-evidence fields during abandonment; refuse committed, verified, abandoned, duplicate-ID, or checkpoint/barrier/source-mismatched owners. The replacement records its actual current scope and need not equal the abandoned owner's scope.
- [x] Revalidate the exact checkpoint, barrier, source identity, and prepared recovery record in the target transaction. Continue an interrupted record only through explicit, validated prepared recovery resume; abandoning or replacing it requires a separately authorized new recovery ID.
- [x] Require complete exact unified run/table progress proof and unchanged source scope before atomically updating the checkpoint, superseding the exact barrier, and marking the recovery `committed`.
- [x] `activate-lost-binlog` accepts only the authorized existing `prepared` record at its exact retained boundary. It derives the original table scope only from nonempty, identical persisted prerequisite and `Rows` table-name sets for that recovery ID, reconstructs current inventory only for those tables and their FK children, and requires the exact immutable prepared inventory hash before activation and again immediately before commit. A changed original definition, incomplete/different progress set, or wrong scope fails closed.
- [x] Audit current source tables outside the recorded original scope as `additional_source_tables`; do not claim them synchronized. Their changes remain eligible for replay from the same original boundary. Activation does not require a binlog table-creation scan.
- [x] Activation uses the existing atomic checkpoint/recovery-record commit. Its durable proof truthfully records `data_converged=true`, `schema_converged=false`, and `final_constraints_deferred=true`; it does not claim final FK convergence.
- [x] Preserve the historical journal row; active-barrier selection excludes only the exact committed or verified recovery identity and barrier coordinates/raw-SQL hash; abandoned history never suppresses the journal barrier.
- [x] Roll back the transition on checkpoint/recovery commit failure.
- [x] Fail closed on interruption or error before proof/commit: no checkpoint or barrier transition is allowed without complete proof and exact CAS revalidation.
- [ ] Verify interrupted full-scope reconciliation and live stream restart behavior before deploying the phased constraint-preserving path. Disposable proof covers all-complete legacy-row reuse without phase-table access and an injected post-commit phase-cursor failure with same-ID resume; it does not claim a production recovery run.

### Verification

- [x] Prove against disposable endpoints that a committed exact recovery preserves its historical journal row yet permits a later stream row and checkpoint advancement.
- [ ] Restart the production stream immediately after a committed transition and prove readiness, checkpoint advancement, and restart cessation.
- [ ] Persist measured post-transition schema/data validation and mark recovery `verified` only at zero unresolved drift.
- [ ] Execute the recovery in production. This branch documentation does not claim that it happened.

## How it works

- [Checkpoint control plane](../checkpoints.md#lost-binlog-recovery-control-plane) — authoritative checkpoint, journal, lease, and recovery-record rules.
- [Validation](../validation.md#lost-binlog-recovery-evidence) — reconciliation evidence and verification gates.
- [Design](../design.md#lost-binlog-recovery) — availability-first skip boundary, committed reads, and replay boundary.
- `docs/stream-recovery-records-bootstrap.sql` — bootstrap for `cdc.stream_recovery_records` and its immutability guards.

## Implementation inventory

- `src/lost_binlog_recovery.rs` — authorization, per-attempt source-scope validation, fresh recovery, validated prepared recovery resume, completed-row activation at the original boundary, reconciliation orchestration, and atomic transition.
- `src/lost_binlog_recovery_store.rs` — target-side CAS reads, exact-barrier owner locking, immutable prepared insert, abandoned replacement transition, checkpoint update, commit, and exact barrier exclusion.
- `scripts/lost-binlog-integration-harness.py` — disposable exact-authorization refusal, full-scope reconciliation, unresolved-barrier no-overtake, committed recovery, and post-recovery replay proof.
- `src/mysql_client.rs` — non-locking MariaDB coordinate capture and narrowly scoped payment replay acknowledgement.
- `src/mysql_client/payment_replay.rs` — source/target identity and full-row proof for acknowledged payment trigger replays.
- `src/inventory/reader.rs` — committed source metadata reads.
- `src/sync/orchestrate.rs`, `src/sync/phased_run.rs`, `src/sync/chunk.rs`, and `src/sync/phase_progress.rs` — unified prerequisite schema, component-locked source-authoritative row phases, additive cursor persistence, legacy-progress reuse, and final constraints.
- `src/sync_schema.rs` — prerequisite and final schema-stage planning/execution.
- `docs/stream-recovery-records-bootstrap.sql` — recovery-record table, active-barrier identity, guards, inventory procedure, and grants.
- `docs/stream-recovery-records-abandoned-replacement-migration.sql` — target-only live-schema migration with duplicate-owner preflight and prepared-row postflight.
- `docs/sync-phase-progress-bootstrap.sql` — additive default phase cursor table and exact target sync-account phase read/write grant. Current phased runtime still issues idempotent creation, so this bootstrap does not remove its existing separately reviewed `CREATE ON cdc.*` requirement.

## Tests asserting this spec

- `scripts/resume-recovery-integration-harness.py` — hard-kill/restart with a completed table and partial row cursor; completed-row activation; unchanged prepared identity/boundary; activation permits audited later source tables without claiming them synchronized; post-capture changes replay from that original boundary; authorization, changed-original-scope, expired-boundary, terminal-state, and atomic-rollback refusal.
- `tests/lost_binlog_resume_cli.rs` — explicit resume command/help and authorization validation before connection.

- `src/lost_binlog_recovery.rs` and `src/main/tests/lost_binlog_unified.rs` — captured source evidence reuse, unified run configuration, exact run/table progress proof, unchanged-scope proof, replacement owner abandonment, rollback/refusal cases, exact old-state validation, duplicate/non-advancing refusal, and exact historical-barrier supersession.
- `src/sync/chunk.rs`, `src/sync/orchestrate.rs`, and `src/sync_schema.rs` — locked chunk boundaries, staged schema/row progress, and final-constraint behavior.
- `src/lost_binlog_recovery_store.rs` — immutable prepared insert, locked CAS queries, abandoned parsing/replacement SQL, checkpoint update, committed transition, and exact barrier predicates.
- `scripts/cdc-integration-harness.py` — disposable payment snapshot-ahead proof plus negative different-primary-key, different-owner, source-current-mismatch, and unrelated-signal cases. Evidence: `/tmp/claude/cdc-payment-policy-green.log`.

## Known gaps (current cycle)

- [ ] Run bootstrap and startup validation against the target with stream writers stopped.
- [ ] Prove the full CLI path with the complete configured scope and current committed source state; production success is not claimed by this branch.
- [ ] Complete the authorized production recovery or prepared resume, replace superseded runtimes, and retain execution evidence in the deployment repository; production completion is not claimed.
- [ ] Complete post-transition schema/data validation with zero unresolved drift and record `verified` evidence.

## Out of scope

- Generic checkpoint setters or barrier bypasses.
- Manual checkpoint/journal edits or ad-hoc SQL.
- Recovery of an arbitrary checkpoint, source identity, journal event, or partial table scope.
- Claiming source-history replay or target freshness for the purged interval.
- Expanding MariaDB-to-MySQL statement compatibility; that belongs to the statement-event coverage work.
