# Lost-binlog recovery

`recover-lost-binlog` is the audited availability-first transition for a stream whose durable MariaDB binlog checkpoint names purged history. It consumes an operator JSON authorization, captures one non-locking MariaDB binlog coordinate plus one committed source evidence set, then runs the unified staged sync for the exact source-table scope before atomically advancing only the authorized stream checkpoint and superseding only the exact historical journal barrier. `resume-lost-binlog` resumes only that same durable prepared transition. Binlog events after the captured coordinate remain eligible for replay after recovery. The implementation is present on this branch; deployment, production execution, restart health, and post-transition verification are not claimed here.

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
- [x] Invoke one unified staged sync for every source-table in the captured inventory, using run ID `recovery_id`, configured `--parallelism` (default `1`), and the configured `cdc.sync_runs` progress table.
- [x] Preserve the replay boundary: source commits after the captured coordinate remain eligible for stream binlog replay after recovery advances the checkpoint.
- [x] Use the unified stages for prerequisite schema convergence, target-WRITE-locked source-authoritative chunks, durable per-stage/per-table progress, and final constraint convergence.
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
- [x] Preserve the historical journal row; active-barrier selection excludes only the exact committed or verified recovery identity and barrier coordinates/raw-SQL hash; abandoned history never suppresses the journal barrier.
- [x] Roll back the transition on checkpoint/recovery commit failure.
- [x] Fail closed on interruption or error before proof/commit: no checkpoint or barrier transition is allowed without complete proof and exact CAS revalidation.
- [ ] Verify interrupted full-scope reconciliation and live stream restart behavior.

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

- `src/lost_binlog_recovery.rs` — authorization, per-attempt source-scope validation, fresh recovery, validated prepared recovery resume at the original boundary, reconciliation orchestration, and atomic transition.
- `src/lost_binlog_recovery_store.rs` — target-side CAS reads, exact-barrier owner locking, immutable prepared insert, abandoned replacement transition, checkpoint update, commit, and exact barrier exclusion.
- `scripts/lost-binlog-integration-harness.py` — disposable exact-authorization refusal, full-scope reconciliation, unresolved-barrier no-overtake, committed recovery, and post-recovery replay proof.
- `src/mysql_client.rs` — non-locking MariaDB coordinate capture.
- `src/inventory/reader.rs` — committed source metadata reads.
- `src/sync/orchestrate.rs`, `src/sync/run.rs`, and `src/sync/chunk.rs` — unified prerequisite schema, locked source-authoritative row chunks, durable progress, and final constraints.
- `src/sync_schema.rs` — prerequisite and final schema-stage planning/execution.
- `docs/stream-recovery-records-bootstrap.sql` — recovery-record table, active-barrier identity, guards, inventory procedure, and grants.
- `docs/stream-recovery-records-abandoned-replacement-migration.sql` — target-only live-schema migration with duplicate-owner preflight and prepared-row postflight.

## Tests asserting this spec

- `scripts/resume-recovery-integration-harness.py` — hard-kill/restart with a completed table and partial row cursor; unchanged prepared identity/boundary; post-capture changes replayed from that original boundary; authorization, changed-scope, expired-boundary, and terminal-state refusal.
- `tests/lost_binlog_resume_cli.rs` — explicit resume command/help and authorization validation before connection.

- `src/lost_binlog_recovery.rs` and `src/main/tests/lost_binlog_unified.rs` — captured source evidence reuse, unified run configuration, exact run/table progress proof, unchanged-scope proof, replacement owner abandonment, rollback/refusal cases, exact old-state validation, duplicate/non-advancing refusal, and exact historical-barrier supersession.
- `src/sync/chunk.rs`, `src/sync/orchestrate.rs`, and `src/sync_schema.rs` — locked chunk boundaries, staged schema/row progress, and final-constraint behavior.
- `src/lost_binlog_recovery_store.rs` — immutable prepared insert, locked CAS queries, abandoned parsing/replacement SQL, checkpoint update, committed transition, and exact barrier predicates.

## Known gaps (current cycle)

- [ ] Run bootstrap and startup validation against the target with stream writers stopped.
- [ ] Prove the full CLI path with the complete configured scope and current committed source state; production success is not claimed by this branch.
- [ ] Execute the separately authorized replacement recovery `cdc-lost-binlog-2026-08-13-drop-trigger-retry3` and replace any superseded stream runtime; production completion is not claimed.
- [ ] Complete post-transition schema/data validation with zero unresolved drift and record `verified` evidence.

## Out of scope

- Generic checkpoint setters or barrier bypasses.
- Manual checkpoint/journal edits or ad-hoc SQL.
- Recovery of an arbitrary checkpoint, source identity, journal event, or partial table scope.
- Claiming source-history replay or target freshness for the purged interval.
- Expanding MariaDB-to-MySQL statement compatibility; that belongs to the statement-event coverage work.
