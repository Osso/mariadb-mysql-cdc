# Lost-binlog recovery

`recover-lost-binlog` is the audited availability-first transition for a stream whose durable MariaDB binlog checkpoint names purged history. It consumes an operator JSON authorization, captures a non-locking MariaDB binlog coordinate, reconciles committed source state for the attempt's actual scope even when the target has extra base tables, then performs recovery-only schema convergence before atomically advancing only the authorized stream checkpoint and superseding only the exact historical journal barrier. Binlog events after the captured coordinate are replayed by the stream after recovery. The implementation is present on `ad-drop-trigger-lost-binlog-recovery`; production execution and post-transition verification are not claimed here.

## What it must do

### Authorization and scope

- [x] Read an authorization JSON containing the exact old checkpoint, exact journal barrier and SQL, source identity, checkpoint name, recovery ID, operator identity, and reason.
- [x] Reject a configured source identity or checkpoint name that does not match the authorization.
- [x] Compute and record the current source scope hash for each attempt; when authorization supplies a scope hash, reject an authorization hash that differs from that attempt's current source scope.
- [x] Reject any configured source table whose engine is not InnoDB.

### Anchored reconciliation

- [x] Acquire the stream lease before recovery state changes.
- [x] Capture the MariaDB binlog coordinate with ordinary non-locking source reads; source recovery must not require `FLUSH TABLES WITH READ LOCK`, `UNLOCK TABLES`, `LOCK TABLES`, or `RELOAD`.
- [x] Reconcile normally committed source rows and schema evidence without a long-lived cross-table transaction or repeatable-read snapshot.
- [x] Begin full-scope row synchronization directly from table inventories; do not run source or target `COUNT(*)` pre-scans.
- [x] When configured with `--parallelism`, run independent full-scope tables concurrently within delete/insert/update/verify phase barriers, never crossing foreign-key dependency levels; each worker reads from the configured source endpoint.
- [x] Preserve the replay boundary: source commits after the captured coordinate remain eligible for stream binlog replay after recovery advances the checkpoint.
- [x] Reconcile every current source-scope table, including target-only orphan rows; target-only target tables do not narrow the recovery data plan, and generic `repair-drift` remains strict about its source/target inventory contract.
- [x] Before data reconciliation, converge only required repair prerequisites: add missing source tables, columns, primary keys, and indexes. This phase permits no `DROP`, target-only table removal, foreign-key convergence, or CHECK-constraint convergence.
- [x] Run final recovery-only schema convergence after data reconciliation: drop target-only base tables child-before-parent with normal foreign-key enforcement, fail closed on cycles or source-table references to target-only parents, and converge remaining source tables and constraints.
- [x] Require the final target base-table inventory to exactly match the source inventory before commit.
- [x] Refuse checkpoint transition when any table is skipped, unsupported, unresolved, or the final target inventory differs from source.
- [ ] Prove the complete live CLI path against the production-shaped full scope, including data repair before final destructive/foreign-key/CHECK convergence.

### Durable transition

- [x] Insert an immutable `prepared` recovery record containing old state, new coordinate, source identity, scope, operator, reason, and evidence; a prepared recovery ID is non-resumable.
- [x] Lock the checkpoint, exact barrier, new recovery ID, and exact-barrier recovery owner in one preparation transaction.
- [x] When a separately authorized recovery ID replaces a `prepared` owner for the exact checkpoint, barrier, and source identity, atomically mark only the old row `abandoned` with server-generated `abandoned_at` and evidence binding both recovery IDs, operator, reason, checkpoint, barrier, source identity, and both attempts' scopes, then insert the replacement `prepared` row.
- [x] Preserve all old identity, scope, and prepared-evidence fields during abandonment; refuse committed, verified, abandoned, duplicate-ID, or checkpoint/barrier/source-mismatched owners. The replacement records its actual current scope and need not equal the abandoned owner's scope.
- [x] Revalidate the exact checkpoint, barrier, source identity, and prepared recovery record in the target transaction; after a prepared failure, reject reuse and require a separately authorized new recovery ID.
- [x] Require complete zero-drift schema/data proof before atomically updating the checkpoint, superseding the exact barrier, and marking the recovery `committed`.
- [x] Preserve the historical journal row; active-barrier selection excludes only the exact committed or verified recovery identity and barrier coordinates/raw-SQL hash; abandoned history never suppresses the journal barrier.
- [x] Roll back the transition on checkpoint/recovery commit failure.
- [x] Fail closed on interruption or error before proof/commit: no checkpoint or barrier transition is allowed without complete proof and exact CAS revalidation; resumability is not claimed.
- [ ] Verify interrupted full-scope reconciliation and live stream restart behavior.

### Verification

- [ ] Restart the stream immediately after a committed transition and prove readiness, checkpoint advancement, and restart cessation.
- [ ] Persist measured post-transition schema/data validation and mark recovery `verified` only at zero unresolved drift.
- [ ] Execute the recovery in production. This branch documentation does not claim that it happened.

## How it works

- [Checkpoint control plane](../checkpoints.md#lost-binlog-recovery-control-plane) — authoritative checkpoint, journal, lease, and recovery-record rules.
- [Validation](../validation.md#lost-binlog-recovery-evidence) — reconciliation evidence and verification gates.
- [Design](../design.md#lost-binlog-recovery) — availability-first skip boundary, committed reads, and replay boundary.
- `docs/stream-recovery-records-bootstrap.sql` — bootstrap for `cdc.stream_recovery_records` and its immutability guards.

## Implementation inventory

- `src/lost_binlog_recovery.rs` — authorization, per-attempt source-scope validation, committed-state boundary, reconciliation orchestration, and atomic transition.
- `src/lost_binlog_recovery_store.rs` — target-side CAS reads, exact-barrier owner locking, immutable prepared insert, abandoned replacement transition, checkpoint update, commit, and exact barrier exclusion.
- `src/mysql_client.rs` — non-locking MariaDB coordinate capture.
- `src/inventory/reader.rs` — committed source metadata reads.
- `src/repair_drift/` — full-scope insert/update/delete and verification phases.
- `src/sync_schema.rs` — repair-prerequisite table/column/key convergence before data repair, then final constraint convergence after repair.
- `docs/stream-recovery-records-bootstrap.sql` — recovery-record table, active-barrier identity, guards, inventory procedure, and grants.
- `docs/stream-recovery-records-abandoned-replacement-migration.sql` — target-only live-schema migration with duplicate-owner preflight and prepared-row postflight.

## Tests asserting this spec

- `src/lost_binlog_recovery.rs` — phase ordering, repair-prerequisite schema before data reconciliation, target-orphan repair before schema/FK convergence, replacement owner abandonment, rollback/refusal cases, exact old-state validation, duplicate/non-advancing refusal, incomplete-proof refusal, atomic rollback, and exact historical-barrier supersession.
- `src/sync_schema.rs` — pre-repair plans add missing columns/keys without scheduling foreign-key constraints.
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
