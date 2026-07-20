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
- [x] After a durably persisted row conflict, roll back, keep the checkpoint
  unchanged, and retry the same transaction in-process with bounded backoff.
- [x] For the exact `globalcomix.sessions` foreign-key error `1452` naming
  `fk_sessions_guest`, persist the conflict first, then validate the source and
  target `guests` identity before retrying. Insert one complete canonical
  23-column source parent only when the target lookup finds no row; accept an
  existing row only when exactly one complete row matches the source image,
  including `guest_id` and `guest_hash`. Compare parent/child ordering using the
  dedicated `UNIX_TIMESTAMP(create_time)` query epoch, never the session-time-zone-rendered canonical timestamp text; source and target recovery connections explicitly set `time_zone='+00:00'` once when each connection is created, before parent reads/writes, while the epoch helper remains excluded from insert and equality.
  The recovery value is reconstructed deterministically from the replayed row image and persisted conflict identity; it is not stored in `cdc.row_conflicts`. Recovery failure returns a contextual typed non-retryable error: no replay, another attempt, or checkpoint advance. Successful child replay writes matching conflict resolution after child DML/checkpoint and before the same target COMMIT; post-commit work only updates process-local cache. Disposable real-database proof remains a
  separate unchecked gap below.
- [x] Stop and fail explicitly on other non-transient errors such as authentication
  failure, missing binlog file, unsupported event type, quarantine, or target
  write failure without durable row-conflict evidence.

Reconnect/backoff applies after transient source loss and after a durable row
conflict. The default stream budget is 12 reconnects after the initial attempt
(13 attempts total); `--max-reconnects 0` disables reconnects, and
`--reconnect-forever true` removes the cap for retryable stream failures,
including persisted row conflicts. Purged or missing source binlogs and other
non-transient failures never use that unbounded path. For the admitted sessions/guests case, recovery runs after
the failed transaction has rolled back and ledger evidence is durable, before the
unchanged checkpoint is replayed. The parent repair itself does not advance the stream
checkpoint; only successful replay advances it. Recovery requires a durable
checkpoint store and fails closed on unsupported scope, missing/colliding/divergent
source or target identity, incomplete source image, connection failure, or target
insert failure. Once strict reconciliation starts, any such failure returns the
recovery error rather than the original persisted-conflict error. Retry eligibility is checked before the recovery callback. An
exhausted retry budget returns the persisted conflict without reading or mutating
the recovery target. This is not generic FK repair or live proof.
It is not an opportunistic TLS-to-plaintext fallback: the current GlobalComix
source uses explicit plaintext mode from the start. Target TLS configuration is
separate; failed target CA loading, chain validation, or required DNS/hostname
identity matching stops immediately.

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

### Replay safety

- [x] Reconnect replay must not re-read the last statement applied before the
  checkpoint boundary.
- [ ] Reconnect replay must be idempotent for statements already applied before
  the checkpoint boundary.
- [ ] Duplicate-key handling may be used only as a secondary safety net; it must
  not be the primary recovery mechanism.
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
- `docs/live-migration-log.md` records observed production stream behavior and
  incidents.

## Implementation inventory

- `src/live/structured_stream/` — production native `mysql_cdc` row/DDL stream,
  transaction boundaries, and event-end checkpoint decisions.
- `src/live/reconnect.rs` — reconnect policy and checkpoint resume semantics.
- `src/table_sync/run.rs` and `src/table_sync/mysql.rs` — bounded exact
  sessions/guest recovery using the canonical 23-column source image.
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
- `src/live/tests.rs` — asserts stream checkpoints are saved after successful
  target apply and not saved after failed target apply.
- `src/live/tests.rs` — asserts transient TLS/connection-reset source failures
  reconnect only while positive attempts remain, `--reconnect-forever true`
  allows unlimited retryable stream failures (including persisted row conflicts),
  and non-transient or purged-binlog failures do not reconnect.
- `src/live/tests/reconnect.rs` — asserts the sessions/guests recovery attempt
  runs only after retry eligibility, observes the unchanged checkpoint, and is
  bounded to one attempt per distinct `SessionsGuestRecovery` request value per
  reconnect loop; this is not ledger-identity deduplication. The same file also
  proves the zero-budget and repeated-request boundaries, but not real database
  reads, inserts, or the production reconnect process.
- `src/table_sync/run.rs` — asserts partial parent images are rejected, the
  absolute create-time epoch controls ordering independently of rendered TIMESTAMP
  text, complete 23-column images preserve required and nullable fields on insert,
  the helper epoch is excluded, and an existing target parent must match the
  complete canonical source image. These are unit tests, not a real source/target
  recovery proof.
- `src/stream_checkpoint.rs` — asserts target checkpoint writes and resume
  selection remain source-identity scoped.

## Known gaps (current cycle)

- [ ] Document and verify the reviewed bootstrap-coordinate provisioning path for
  a new source identity in the Kubernetes deployment.
- [ ] Add a failing test where stream input exits after applying events and the
  next stream resumes from the saved checkpoint.
- [x] Add a failing test where process startup reads an existing checkpoint and
  overrides static startup coordinates.
- [ ] Prove the sessions/guests recovery against disposable real MariaDB/MySQL,
  including source/target identity collisions, recovery failure, parent insert,
  and successful replay/checkpoint advancement. The existing real FK harness
  scenario proves conflict rollback/evidence and manual repair boundaries, not
  this automatic reconnect callback.
- [x] Add a failing test that checkpoint is written only after successful target
  apply.
- [x] Production streaming uses the native client/reconnect loop; the
  `mariadb-binlog --stop-never` helper is not a production dependency.

## Out of scope

- Solving SQL compatibility gaps. Unsupported SQL still belongs to statement or
  row-event handling specs.
- Cutover automation. Reconnect is required before cutover, but endpoint switch
  remains a separate workflow.
