# live-stream-reconnect

`live-stream-reconnect` is the reliability contract for the CDC live stream from
the MariaDB source to the MySQL target. The live stream must survive transient
source connection loss without replaying from static startup coordinates.

## What it must do

### Connection loss

- [x] Detect source stream loss, including TCP reset, EOF, timeout, and client
  process exit. The live GlobalComix source is plaintext-only.
- [x] Reconnect automatically after transient source stream loss.
- [x] Resume from the last durably applied source coordinate, not from the
  manifest's original `--binlog-file` and `--start-position` arguments.
- [x] Apply bounded retry backoff with clear logs for attempt count, delay, and
  last durable coordinate.
- [x] After unsupported or semantically blocked automatic DDL persists its
  journal barrier, keep the checkpoint unchanged and retry the same source
  coordinate in-process indefinitely without consuming the ordinary transport
  retry budget. Never skip the DDL or execute its raw source SQL.
- [x] Treat native ROW INSERT `1062` as success in place; do not reconnect, read
  target state, write conflict evidence, or run repair.
- [x] For every other target row error, roll back the complete source transaction,
  keep the checkpoint unchanged, and return the failure without classifying it as
  transient source loss.
- [x] Stop and fail explicitly on other non-transient errors such as authentication
  failure, missing binlog file, unsupported event type, quarantine, or target row
  failure. Durable automatic-DDL barriers are the explicit exception: they remain
  process-live and retry at the unchanged coordinate.

Reconnect/backoff applies only to transient source loss. Durable automatic-DDL
barriers use a separate process-live retry loop: they retry indefinitely from the
unchanged checkpoint without consuming the ordinary transport retry budget, and
they never skip or raw-execute the source statement. Generic non-DDL quarantine,
mapping, and target row errors remain fatal. Ordinary transport reconnects use a
default budget of 12 after the initial attempt (13 attempts total).
`--max-reconnects 0` disables reconnects unless `--reconnect-forever true` is
set; the latter removes the ordinary transport cap. Purged or missing source
binlogs and other non-transient failures never use that unbounded path.

It is not an opportunistic TLS-to-plaintext fallback: the current GlobalComix
source uses explicit plaintext mode from the start. Target TLS configuration is
separate; failed target CA loading, chain validation, or required DNS/hostname
identity matching stops immediately.

### Serial target transactions

- [x] Execute live target work serially on one initialized Rust `mysql::Conn`.
- [x] Keep each complete source transaction on that connection from `BEGIN`
  through `COMMIT` or `ROLLBACK`.
- [x] Allow the existing target transaction group-size and timeout controls to
  group complete source transactions without introducing concurrent target
  workers.
- [x] Write the target checkpoint in the same target transaction as grouped DML
  and commit both atomically in source order.
- [x] Flush the active grouped transaction when its group-size or timeout limit is
  reached, at binlog rotation, for DDL, at bounded stop, and at stream completion.
- [x] Keep missing-FK and duplicate-parent reconciliation reads inside the active
  target transaction.
- [x] Roll back the active grouped transaction and leave its checkpoint unchanged
  after any non-ignored target row failure.
- [x] Ignore MySQL `1062` only for native INSERT statements; continue later
  statements in the same source transaction and fail on every other row error.

### Durable checkpointing

- [x] Persist the last successfully applied binlog file and position outside the
  running process before acknowledging stream progress.
- [x] Persist the post-event resume position (`end_log_pos`) for statement
  events, not the statement start position, so reconnect does not replay the
  last applied event.
- [ ] Persist GTID when available, alongside file/position.
- [x] On process start, prefer the durable checkpoint over static startup
  coordinates unless an explicit reset flag is provided.
- [x] Never advance the checkpoint before the corresponding target write has
  succeeded.
- [x] Make checkpoint writes atomic so pod eviction or node loss cannot leave a
  partially written checkpoint.
- [ ] For purged-history incidents, use only the audited `recover-lost-binlog`
  transition: exact JSON old-state/barrier authorization, per-attempt source/scope
  validation, non-locking coordinate capture, committed-state full-scope
  reconciliation, and atomic checkpoint plus exact-barrier commit. This is an
  availability-first skip, not replay proof; production execution and
  post-transition verification remain open.

### Replay safety

- [x] Reconnect replay must not re-read the last statement applied before the
  checkpoint boundary.
- [ ] Reconnect replay must be idempotent for statements already applied before
  the checkpoint boundary.
- [x] INSERT `1062` handling is an explicit idempotence rule, not a reconnect or
  target-reconciliation mechanism.
- [x] Preserve binlog order across reconnects.
- [ ] Log every reconnect boundary with previous coordinate, resume coordinate,
  and first applied coordinate after reconnect.

### Architecture

- [x] The production stream must own reconnect and checkpoint semantics in Rust.
- [x] `mariadb-binlog --stop-never` may be used for fixtures, probes, and
  debugging, but must not be the production live-stream architecture unless it
  is wrapped by durable reconnect/resume logic that satisfies this spec.
- [x] Kubernetes restarts must not be required for normal source connection
  recovery.
- [x] A Kubernetes restart must resume from the same durable checkpoint as an
  in-process reconnect.

### Observability

- [ ] Emit structured stream logs for start, progress, reconnect start,
  reconnect success, reconnect failure, checkpoint write, quarantine, and exit.
- [ ] Expose current stream health: connected/disconnected, retry count, last
  applied coordinate, last checkpoint coordinate, and seconds since last event.
- [ ] Make alerting distinguish active restart storms from recovered historical
  restarts.

## How it works

- `docs/checkpoints.md` documents current checkpoint storage.
- `docs/design.md` documents overall CDC architecture.

## Implementation inventory

- `src/live/structured_stream/` — production native `mysql_cdc` row/DDL stream,
  transaction boundaries, and event-end checkpoint decisions.
- `src/live/reconnect.rs` — reconnect policy and checkpoint resume semantics.
- `src/mysql_client.rs` — serial target connection, grouped transaction execution,
  checkpoint writes, and source-authoritative row repair.
- `src/lost_binlog_recovery.rs` and `src/lost_binlog_recovery_store.rs` — audited
  purged-history checkpoint/barrier transition with anchored full-scope repair.
- `src/stream_checkpoint.rs` — target-table checkpoint store.
- `src/live/binlog_command.rs` — text-binlog helper retained for the legacy probe
  and fixture/debug paths, not the production stream.
- `src/main.rs` — CLI options for live streaming, including `--stop-position`.
- `deployment/stream-manifest` in the deployment repository —
  Kubernetes Deployment passes the source identity and target checkpoint table.
  A new source identity still requires an explicitly reviewed binlog file and
  position; the current test manifest relies on a pre-seeded target checkpoint
  and is not a bootstrap proof.

## Tests asserting this spec

- `src/live/tests.rs` — asserts startup prefers an existing stream checkpoint
  over static CLI coordinates.
- `src/lost_binlog_recovery.rs` and `src/lost_binlog_recovery_store.rs` — asserts
  exact old-state validation, duplicate/non-advancing refusal, proof gating,
  atomic rollback, and exact historical-barrier exclusion.
- `src/live/tests.rs` — asserts stream checkpoints are saved after successful
  target apply and not saved after failed target apply.
- `src/live/tests.rs` — asserts ordinary transient TLS/connection-reset source
  failures reconnect only while positive transport attempts remain,
  `--reconnect-forever true` allows unlimited retryable stream failures, and
  non-transient, target, or purged-binlog failures do not reconnect.
- `src/live/structured_stream/tests/ddl_replay.rs` — asserts an unsupported DDL
  barrier keeps the process-live reconnect loop at the unchanged checkpoint,
  with no target execution or checkpoint write.
- `src/live/tests/reconnect.rs` — asserts transient checkpoint reload, stale
  binlog refusal, bounded retry, and non-retryable target failure.
- `src/live/structured_stream/tests/transaction.rs` — asserts grouped serial
  transaction boundaries, row-failure rollback, and checkpoint ordering.
- `src/stream_checkpoint.rs` — asserts target checkpoint writes and resume
  selection remain source-identity scoped.
- `scripts/cdc-integration-harness.py --scenario insert-duplicate-idempotent` —
  runs serial native ROW replay against disposable MariaDB/MySQL endpoints with
  a divergent preexisting target row and no `cdc.row_conflicts` table or
  inventory procedure. It verifies the duplicate leaves that row untouched,
  applies a later same-transaction row, and advances the exact checkpoint.
- `scripts/cdc-integration-harness.py --scenario missing-fk-nested-parent-auto-insert` —
  runs the production binary against disposable MariaDB/MySQL endpoints and
  proves recursive nested parent repair, child retry, and exact serial checkpoint
  completion.

## Known gaps (current cycle)

- [ ] Document and verify the reviewed bootstrap-coordinate provisioning path for
  a new source identity in the Kubernetes deployment.
- [ ] Add a failing test where stream input exits after applying events and the
  next stream resumes from the saved checkpoint.
- [x] Add a failing test where process startup reads an existing checkpoint and
  overrides static startup coordinates.
- [x] Prove serial INSERT `1062` continuation against disposable real
  MariaDB/MySQL endpoints; keep the conflict ledger and repair paths out-of-band
  only.
- [x] Add a failing test that checkpoint is written only after successful target
  apply.
- [x] Production streaming uses the native client/reconnect loop; the
  `mariadb-binlog --stop-never` helper is not a production dependency.

## Out of scope

- Concurrent live target workers, parallel target submission, or a
  `--target-parallel-transactions` option. Existing group-size and timeout
  controls group source transactions only.
- Solving SQL compatibility gaps. Unsupported SQL still belongs to statement or
  row-event handling specs.
- Cutover automation. Reconnect is required before cutover, but endpoint switch
  remains a separate workflow.
