# live-stream-reconnect

`live-stream-reconnect` is the reliability contract for the CDC live stream from
the MariaDB source to the MySQL target. The live stream must survive transient
source connection loss without replaying from static startup coordinates.

## What it must do

### Connection loss

- [x] Detect source stream loss, including TCP reset, TLS reset, EOF, timeout,
  and `mariadb-binlog`/client process exit.
- [x] Reconnect automatically after transient source stream loss.
- [x] Resume from the last durably applied source coordinate, not from the
  manifest's original `--binlog-file` and `--start-position` arguments.
- [x] Apply bounded retry backoff with clear logs for attempt count, delay, and
  last durable coordinate.
- [x] Stop and fail explicitly on non-transient errors such as authentication
  failure, missing binlog file, unsupported event type, quarantine, or target
  write failure.

Reconnect/backoff applies only after an established connection loses transport.
It is not a TLS fallback: failed CA loading, chain validation, or required
DNS/hostname identity matching stops immediately. Reconnect attempts retain the
same TLS configuration; literal IP endpoints continue to skip hostname/IP
identity matching only.

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
- `docs/wiki/systems/live-stream-reconnect.md` should describe final
  implementation details after reconnect handling is built.

## Implementation inventory

- `src/live/structured_stream/` — production native `mysql_cdc` row/DDL stream,
  transaction boundaries, and event-end checkpoint decisions.
- `src/live/reconnect.rs` — reconnect policy and checkpoint resume semantics.
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
  allows unlimited transient reconnects, and non-transient source failures do
  not reconnect.
- `src/stream_checkpoint.rs` — asserts target checkpoint writes and resume
  selection remain source-identity scoped.

## Known gaps (current cycle)

- [ ] Document and verify the reviewed bootstrap-coordinate provisioning path for
  a new source identity in the Kubernetes deployment.
- [ ] Add a failing test where stream input exits after applying events and the
  next stream resumes from the saved checkpoint.
- [x] Add a failing test where process startup reads an existing checkpoint and
  overrides static startup coordinates.
- [x] Add a failing test that checkpoint is written only after successful target
  apply.
- [x] Production streaming uses the native client/reconnect loop; the
  `mariadb-binlog --stop-never` helper is not a production dependency.

## Out of scope

- Solving SQL compatibility gaps. Unsupported SQL still belongs to statement or
  row-event handling specs.
- Cutover automation. Reconnect is required before cutover, but endpoint switch
  remains a separate workflow.
