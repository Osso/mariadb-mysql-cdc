# live-stream-reconnect

`live-stream-reconnect` is the reliability contract for the CDC live stream from
the MariaDB source to the MySQL target. The live stream must survive transient
source connection loss without replaying from static startup coordinates.

## What it must do

### Connection loss

- [ ] Detect source stream loss, including TCP reset, TLS reset, EOF, timeout,
  and `mariadb-binlog`/client process exit.
- [ ] Reconnect automatically after transient source stream loss.
- [ ] Resume from the last durably applied source coordinate, not from the
  manifest's original `--binlog-file` and `--start-position` arguments.
- [ ] Apply bounded retry backoff with clear logs for attempt count, delay, and
  last durable coordinate.
- [ ] Stop and fail explicitly on non-transient errors such as authentication
  failure, missing binlog file, unsupported event type, quarantine, or target
  write failure.

### Durable checkpointing

- [ ] Persist the last successfully applied binlog file and position outside the
  running process before acknowledging stream progress.
- [ ] Persist GTID when available, alongside file/position.
- [ ] On process start, prefer the durable checkpoint over static startup
  coordinates unless an explicit reset flag is provided.
- [ ] Never advance the checkpoint before the corresponding target write has
  succeeded.
- [ ] Make checkpoint writes atomic so pod eviction or node loss cannot leave a
  partially written checkpoint.

### Replay safety

- [ ] Reconnect replay must be idempotent for statements already applied before
  the checkpoint boundary.
- [ ] Duplicate-key handling may be used only as a secondary safety net; it must
  not be the primary recovery mechanism.
- [ ] Preserve binlog order across reconnects.
- [ ] Log every reconnect boundary with previous coordinate, resume coordinate,
  and first applied coordinate after reconnect.

### Architecture

- [ ] The production stream must own reconnect and checkpoint semantics in Rust.
- [ ] `mariadb-binlog --stop-never` may be used for fixtures, probes, and
  debugging, but must not be the production live-stream architecture unless it
  is wrapped by durable reconnect/resume logic that satisfies this spec.
- [ ] Kubernetes restarts must not be required for normal source connection
  recovery.
- [ ] A Kubernetes restart must resume from the same durable checkpoint as an
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

- `src/live.rs` — current live stream, binlog extraction, statement apply loop,
  and stream progress logging.
- `src/live/binlog_command.rs` — current `mariadb-binlog` command construction.
- `src/checkpoint.rs` — file-backed durable checkpoint model.
- `src/main.rs` — CLI options for live streaming.
- `deployment/stream-manifest` in the deployment repository —
  Kubernetes Deployment currently passing static startup coordinates.

## Tests asserting this spec

- No automated tests assert reconnect behavior yet.

## Known gaps (current cycle)

- [ ] Add a failing test where stream input exits after applying events and the
  next stream resumes from the saved checkpoint.
- [ ] Add a failing test where process startup reads an existing checkpoint and
  overrides static startup coordinates.
- [ ] Add a failing test that checkpoint is written only after successful target
  apply.
- [ ] Replace or wrap the `mariadb-binlog --stop-never` production path so a TLS
  reset does not terminate CDC progress.

## Out of scope

- Solving SQL compatibility gaps. Unsupported SQL still belongs to statement or
  row-event handling specs.
- Cutover automation. Reconnect is required before cutover, but endpoint switch
  remains a separate workflow.
