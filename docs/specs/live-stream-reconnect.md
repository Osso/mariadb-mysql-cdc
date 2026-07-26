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
- [x] For an `INSERT` into `globalcomix.payments` that returns MySQL `1644` with
      the exact message `This external payment has already been applied to a
      previous order`, roll back and inspect the target row selected by source
      primary key. Exactly one row matching `id`, `order_id`,
      `payment_service_id`, `transaction_id`, `original_transaction_id`, and
      `authorization_id` is treated as already applied; stage resolution and
      allow replay/checkpoint commit. Missing, ambiguous, or divergent identity
      remains durable conflict evidence and retries from the unchanged
      checkpoint. Other trigger errors remain fatal.
- [x] For the exact `globalcomix.sessions` foreign-key error `1452` naming
  `fk_sessions_guest`, persist the conflict first, then validate the source and
  target `guests` identity before retrying. Insert one complete canonical
  23-column source parent only when the target lookup finds no row; accept an
  existing row only when exactly one complete row matches the source image,
  including `guest_id` and `guest_hash`.
- [x] For the exact `globalcomix.home_feed_card_slides` foreign-key error `1452`
  naming `fk_hfcs_card` (`card_id` → `home_feed_cards.id`), persist the conflict
  first, then validate positive child IDs and the complete source parent image
  before retrying. Insert the canonical parent only when the target has no
  matching identity; accept one exact existing row and fail closed otherwise.
  Both paths compare parent/child ordering using the dedicated
  `UNIX_TIMESTAMP(create_time)` query epoch, never rendered timestamp text;
  recovery connections set `time_zone='+00:00'`, and the helper epoch is excluded
  from insert and equality. Recovery values are reconstructed deterministically
  from replay input and conflict context, not stored in `cdc.row_conflicts`.
  Recovery failure returns a contextual typed error; when exact-parent retry is
  enabled, the reconnect loop retries it while preserving the ordinary transport
  budget and unchanged checkpoint for another attempt. The failed recovery
  performs no replay or checkpoint advance. Successful child replay writes matching
  conflict resolution after child DML/checkpoint and before the same target
  COMMIT; post-commit work only updates process-local cache. Disposable
  real-database proof remains a separate unchecked gap below.
- [x] Stop and fail explicitly on other non-transient errors such as authentication
  failure, missing binlog file, unsupported event type, quarantine, or target
  write failure without durable row-conflict evidence.

Reconnect/backoff applies after transient source loss and after a durable row
conflict. Ordinary transport reconnects use a default budget of 12 after the
initial attempt (13 attempts total). `--max-reconnects 0` disables reconnects
unless `--reconnect-forever true` is set; the latter removes the ordinary
transport cap and admits exact-parent retries even when the max is zero. An
exact-parent retry is admitted only with a positive `max-reconnects` setting or
reconnect-forever, and its recovery success or failure preserves the ordinary
transport budget.
Thus repeated exact-parent/recovery failures can exceed `max-reconnects`.
Purged or missing source binlogs and other non-transient failures never use that
unbounded path. For either admitted exact parent-recovery case, recovery runs after
the failed transaction has rolled back and ledger evidence is durable, before the
unchanged checkpoint is replayed. Failed recovery attempts remain eligible on later
loops; after one succeeds, a later retry of the same request skips parent mutation
but still replays from the unchanged checkpoint. The parent repair itself does not
advance the stream checkpoint; only successful replay advances it. Recovery requires
a durable checkpoint store and fails closed on unsupported scope, missing/colliding/divergent
source or target identity, incomplete source image, connection failure, or target
insert failure. Once strict reconciliation starts, any such failure returns the
recovery error rather than the original persisted-conflict error. Retry eligibility is checked before the recovery callback. With both ordinary reconnects disabled and reconnect-forever false, the persisted conflict returns without reading or mutating the recovery target. This is not generic FK repair or live proof.
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

## Implementation inventory

- `src/live/structured_stream/` — production native `mysql_cdc` row/DDL stream,
  transaction boundaries, and event-end checkpoint decisions.
- `src/live/reconnect.rs` — reconnect policy and checkpoint resume semantics.
- `src/table_sync/run.rs` and `src/table_sync/mysql.rs` — bounded exact parent
  recovery for sessions/guests and home-feed cards using canonical source images.
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
- `src/live/tests.rs` — asserts ordinary transient TLS/connection-reset source
  failures reconnect only while positive transport attempts remain,
  `--reconnect-forever true` allows unlimited retryable stream failures, and
  non-transient or purged-binlog failures do not reconnect. Exact-parent retry
  budget behavior is covered separately below.
- `src/live/tests/reconnect.rs` — asserts exact parent recovery is admitted only
  after its retry gate, observes the unchanged checkpoint, retries failed
  recoveries beyond the ordinary transport budget, and mutates each distinct
  `ExactParentRecovery` value at most once after success; this is not
  ledger-identity deduplication. The same file proves the zero-budget and
  repeated-request boundaries, but not real database reads, inserts, or the
  production reconnect process.
- `src/table_sync/run.rs` — asserts partial parent images are rejected, the
  absolute create-time epoch controls ordering independently of rendered TIMESTAMP
  text, canonical guest and home-feed-card images preserve required and nullable
  fields on insert, the helper epoch is excluded, and an existing target parent
  must match the complete source image. These are unit tests, not a real
  source/target recovery proof.
- `src/stream_checkpoint.rs` — asserts target checkpoint writes and resume
  selection remain source-identity scoped.

## Known gaps (current cycle)

- [ ] Document and verify the reviewed bootstrap-coordinate provisioning path for
  a new source identity in the Kubernetes deployment.
- [ ] Add a failing test where stream input exits after applying events and the
  next stream resumes from the saved checkpoint.
- [x] Add a failing test where process startup reads an existing checkpoint and
  overrides static startup coordinates.
- [ ] Prove both exact parent recoveries against disposable real MariaDB/MySQL,
  including source/target identity collisions, recovery failure, parent insert,
  and successful replay/checkpoint advancement. The existing real FK harness
  scenario proves conflict rollback/evidence and manual repair boundaries, not
  these automatic reconnect callbacks.
- [x] Add a failing test that checkpoint is written only after successful target
  apply.
- [x] Production streaming uses the native client/reconnect loop; the
  `mariadb-binlog --stop-never` helper is not a production dependency.

## Out of scope

- Solving SQL compatibility gaps. Unsupported SQL still belongs to statement or
  row-event handling specs.
- Cutover automation. Reconnect is required before cutover, but endpoint switch
  remains a separate workflow.
