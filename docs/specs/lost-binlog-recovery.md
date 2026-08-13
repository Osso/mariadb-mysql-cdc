# Lost-binlog recovery

`recover-lost-binlog` is the audited availability-first transition for a stream whose durable MariaDB binlog checkpoint names purged history. It consumes an operator JSON authorization, reconciles the complete configured scope from one MariaDB consistent snapshot, then atomically advances only the authorized stream checkpoint and supersedes only the exact historical journal barrier. The implementation is present on `ad-drop-trigger-lost-binlog-recovery`; production execution and post-transition verification are not claimed here.

## What it must do

### Authorization and scope

- [x] Read an authorization JSON containing the exact old checkpoint, exact journal barrier and SQL, source identity, checkpoint name, recovery ID, operator identity, and reason.
- [x] Reject a configured source identity or checkpoint name that does not match the authorization.
- [x] Compute the current source scope hash and reject an authorization hash that differs.
- [x] Reject any configured source table whose engine is not InnoDB.

### Anchored reconciliation

- [x] Acquire the stream lease before recovery state changes.
- [x] Hold `FLUSH TABLES WITH READ LOCK` only while opening one MariaDB `REPEATABLE READ` consistent snapshot and reading its current binlog coordinate.
- [x] Keep that source snapshot transaction open for full-scope source reads, insert, update, delete, and verification phases.
- [x] Capture source table, index, foreign-key, check, view, trigger, routine, and event evidence through that same snapshot connection; independent live source metadata reads are forbidden.
- [x] Reconcile target data, including target-only orphan rows, before creating foreign keys.
- [x] Run final schema convergence, including foreign-key creation, after data reconciliation; schema convergence must gate the atomic transition.
- [x] Refuse checkpoint transition when any table is skipped, unsupported, or unresolved.
- [ ] Prove the complete live CLI path against the production-shaped full scope, including data repair before final schema/FK convergence.

### Durable transition

- [x] Insert an immutable `prepared` recovery record containing old state, new coordinate, source identity, scope, operator, reason, and evidence.
- [x] Revalidate the exact checkpoint, barrier, source identity, scope, and prepared recovery record under transaction locks.
- [x] Atomically update the checkpoint and mark the recovery `committed` only after reconciliation proof succeeds.
- [x] Preserve the historical journal row; active-barrier selection excludes only the exact committed recovery identity and barrier coordinates/raw-SQL hash.
- [x] Roll back the transition on checkpoint/recovery commit failure.
- [x] Reject duplicate recovery IDs and non-advancing source coordinates.
- [ ] Verify interrupted/resumable full-scope reconciliation and live stream restart behavior.

### Verification

- [ ] Restart the stream immediately after a committed transition and prove readiness, checkpoint advancement, and restart cessation.
- [ ] Persist measured post-transition schema/data validation and mark recovery `verified` only at zero unresolved drift.
- [ ] Execute the recovery in production. This branch documentation does not claim that it happened.

## How it works

- [Checkpoint control plane](../checkpoints.md#lost-binlog-recovery-control-plane) — authoritative checkpoint, journal, lease, and recovery-record rules.
- [Validation](../validation.md#lost-binlog-recovery-evidence) — reconciliation evidence and verification gates.
- [Design](../design.md#lost-binlog-recovery) — availability-first skip boundary and source snapshot fence.
- `docs/stream-recovery-records-bootstrap.sql` — bootstrap for `cdc.stream_recovery_records` and its immutability guards.

## Implementation inventory

- `src/lost_binlog_recovery.rs` — authorization, source/scope validation, snapshot fence, reconciliation orchestration, and atomic transition.
- `src/lost_binlog_recovery_store.rs` — target-side CAS reads, immutable prepared-record insert, checkpoint update, commit, and exact barrier exclusion.
- `src/mysql_client.rs` — MariaDB consistent-snapshot transaction and source coordinate capture.
- `src/inventory/reader.rs` — snapshot-backed source metadata reader on the persistent transaction connection.
- `src/repair_drift/` — full-scope insert/update/delete and verification phases.
- `src/sync_schema.rs` — final schema convergence and foreign-key creation after anchored data repair.
- `docs/stream-recovery-records-bootstrap.sql` — recovery-record table, guards, inventory procedure, and grants.

## Tests asserting this spec

- `src/lost_binlog_recovery.rs` — phase ordering, target-orphan repair before schema/FK convergence, exact old-state validation, duplicate/non-advancing refusal, incomplete-proof refusal, atomic rollback, and exact historical-barrier supersession.
- `src/lost_binlog_recovery_store.rs` — immutable prepared insert, locked CAS queries, checkpoint update, committed transition, and exact barrier predicates.

## Known gaps (current cycle)

- [ ] Run bootstrap and startup validation against the target with stream writers stopped.
- [ ] Prove the full CLI path with the complete configured scope and current source snapshot; production success is not claimed by this branch.
- [ ] Execute the authorized recovery and replace any superseded stream runtime.
- [ ] Complete post-transition schema/data validation with zero unresolved drift and record `verified` evidence.

## Out of scope

- Generic checkpoint setters or barrier bypasses.
- Manual checkpoint/journal edits or ad-hoc SQL.
- Recovery of an arbitrary checkpoint, source identity, journal event, or partial table scope.
- Claiming source-history replay or target freshness for the purged interval.
- Expanding MariaDB-to-MySQL statement compatibility; that belongs to the statement-event coverage work.
