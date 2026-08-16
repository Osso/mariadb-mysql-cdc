# Row Conflict Handling

The structured stream applies MariaDB ROW/FULL events by source primary key. A
secondary-unique conflict must not mutate the target row that owns the
conflicting secondary key. Implementation detail for superseded release proofs:
[superseded release recovery](../wiki/systems/superseded-release-recovery.md).

## Current behavior

- [x] Skip a row event whose table map column count does not match the resolved schema, recording
      the table, both counts, and the coordinate as `cdc_row_event_schema_skipped`. The event
      describes the table as it was when written, this source adds columns mid-table, and
      `binlog_row_metadata` is `NO_LOG`, so no column names exist to map by; mapping the values onto
      the leading columns shifts every later value into the wrong column. The table map is ignored
      for as long as it stands, so its row events are skipped rather than stopping the stream, and a
      later full data sync supplies those rows.
- [x] Build plain `INSERT` statements with the explicit source primary key.
- [x] Never use `ON DUPLICATE KEY UPDATE` for source inserts.
- [x] Classify native ROW `1062` as durable repair debt unless
      `ignore-duplicate` proves exact source/target row equality; admitted NOT
      NULL, foreign-key, and CHECK constraint failures are also durable debt.
- [x] Classify an `INSERT` into `globalcomix.payments` that returns MySQL
      `1644` with the exact message `This external payment has already been
      applied to a previous order` as a supported duplicate-trigger conflict.
      After rollback, require exactly one target row matching the source on the
      stable identity columns `id`, `order_id`, `payment_service_id`,
      `transaction_id`, `original_transaction_id`, and `authorization_id`.
      Treat that exact match as already applied: stage conflict resolution and
      allow replay and checkpoint commit. A missing, ambiguous, or divergent
      identity remains durable conflict evidence and aborts with the checkpoint
      unchanged. Other `1644` errors, other tables, and non-`INSERT` changes do
      not receive this classification.
- [x] Under `ignore-duplicate`, skip a native ROW `INSERT` `1062` only when the
      target row fetched by source primary key exactly equals the source row;
      divergent `ROW INSERT` values and every non-`INSERT` `1062` unique conflict
      persist evidence and abort. Only equal `ROW INSERT` duplicates continue;
      the default `error` policy fails native row duplicates.
- [x] A secondary-unique exception is a superseded historical insert on
      `globalcomix.users`: a ROW `INSERT` whose duplicate index is exactly
      `users.name` may be deferred only as exactly one row-level candidate. At
      XID, the verifier retains the complete historical image, reads
      `SHOW MASTER STATUS` before `START TRANSACTION WITH CONSISTENT SNAPSHOT`,
      and treats that pre-snapshot source coordinate as a conservative lower
      bound for the snapshot contents; the lower bound must be strictly beyond
      the candidate transaction. It requires exactly one complete source row
      for the historical primary key and exactly one
      complete source row owning the historical name, with the historical
      primary key no longer owning that name and the owner having a different
      primary key. If the primary moved away and no current source row owns the
      historical name, classify the candidate as ordinary unresolved debt and
      perform no superseded repair; multiple matching source owners remain
      fail-closed. The active target transaction re-reads both identities with
      `SELECT ... FOR UPDATE`; complete canonical source and target row hashes
      must match. Only then is that insert treated as a no-op; later rows in
      the same source transaction still apply. The XID checkpoint, exact
      conflict observation/resolution evidence, and remaining row effects are
      committed atomically. Any second candidate, failed predicate, invalid
      checkpoint predecessor, or commit failure fails closed: target effects and
      checkpoint advancement roll back, then all unresolved observations are
      persisted through the independent conflict store; rollback or
      evidence-persistence failures are surfaced. Every other secondary-unique
      conflict keeps the ordinary abort path.
- [x] The superseded historical `globalcomix.comics` `ROW INSERT` exception is
      limited to the exact `comics.slug` index. It requires one candidate, a
      complete historical image, a source snapshot beyond the candidate
      transaction, and complete current
      source/target equality for the historical primary row. The current source
      and locked target unique owner must have the same primary key and slug;
      unrelated mutable owner-field drift does not reject the proof. If typed
      verification determines that the source primary still owns the historical
      identity, classify the candidate as ordinary unresolved reconciliation
      debt: record observation evidence, run no superseded repair SQL, and commit
      remaining row effects with the XID checkpoint. The observation/resolution
      evidence, remaining row effects, and XID checkpoint commit atomically for
      a proved supersession, while any other failed predicate or commit failure
      rolls back and leaves the conflict unresolved. The existing
      `globalcomix.users` / `users.name` proof retains full owner-row equality
      and is unchanged.
- [x] When the sole deferred secondary-unique candidate verifies as ordinary
      current-owner reconciliation debt, allow ordinary conflicts from the same
      source transaction to remain skipped only when every observation and the
      XID checkpoint commit atomically with no repair SQL. This covers the
      production `mysqld-bin.002858:859898126–859901371` ordering where the
      `users.name` conflict precedes dependent `users_profiles` FK debt. If the
      deferred candidate requires repair, any coexisting ordinary conflict still
      fails closed and all observations remain unresolved.
- [x] The superseded historical `globalcomix.releases` `ROW INSERT` FK proof is
      limited to the exact approved category transaction
      `mysqld-bin.002709:515816736–515824875` (`releases_ibfk_2`) and visibility
      transaction `mysqld-bin.002709:531921570–531929925` (`releases_ibfk_3`,
      candidate event `531921789`). The exact FK child/parent identity is
      required; the complete historical release image is retained; later source
      history must show a changed parent value; and exactly one current source
      release, matching source parent, and locked target parent identity must be
      proven. If the target release is absent, install the complete current
      source release row; if present, require its full hash to equal current
      source. Preserve the current parent identity without updating or deleting
      the parent. Remaining transaction effects, conflict observation/resolution
      evidence, and the XID checkpoint commit atomically. Any failed proof,
      predecessor, coordinate/FK scope, or commit check fails closed, rolls back
      target effects and checkpoint advancement, and leaves unresolved evidence.
- [x] Successful equal native ROW `INSERT` no-ops and successful
      `replace-divergent-pk` replacements never create a new ledger row; they
      stage resolution of an already-recorded unresolved row only.
- [x] Staged success resolution matches only `source_identity`, schema, table,
      and the canonical source primary-key JSON used by observation; source and
      schema records remain isolated.
- [x] Resolution is finalized only after the target transaction and its
      checkpoint commit; rollback or commit/checkpoint failure leaves the
      existing conflict unresolved.
- [x] Under the explicit `replace-divergent-pk` policy, an unequal native ROW
      `INSERT` duplicate is replaceable only when MySQL identifies `PRIMARY`:
      read exactly one target row by source primary key, update every writable
      source-image column by that primary-key predicate, and require exactly one
      matched target row. Missing/multiple PK rows or any other update count persist
      conflict evidence and abort without checkpoint advancement.
      Foreign-key, CHECK, and replacement-update conflicts never use this path
      and remain durable skipped conflicts. Secondary-unique conflicts are also
      recorded and skipped, except for the separately specified
      superseded `globalcomix.users`/`users.name` and
      `globalcomix.comics`/`comics.slug` insert proofs above. The accepted
      policy risk is overwriting the divergent target row. Replacement keeps applying rows and
      may checkpoint; if its enclosing target transaction later rolls back, the
      independent ledger evidence survives while the replacement itself rolls back.
- [x] Stage supported constraint-conflict observations within the source
      transaction; at its XID, finalize their source-transaction end
      coordinates, persist the unresolved observations through the independent
      control-plane connection, then commit the transaction's remaining row
      effects and advance the XID checkpoint past the conflicting rows. The
      skipped rows are divergence the ledger owns; repair happens out of band.
      Retrying from the unchanged checkpoint is forbidden here because replaying
      the same rows cannot change the target, so the stream would never leave
      the position - unbounded under `--reconnect-forever`. Failure to persist
      the evidence aborts with the checkpoint unchanged, because advancing past a
      divergence that was not recorded would lose it silently. Validate every
      unresolved ledger identity once during stream startup; the per-conflict XID
      path must not re-read the entire ledger before committing recorded debt and
      its checkpoint. Ordinary transport reconnects default to 12 after the
      initial attempt (13 attempts total); `--max-reconnects 0` disables them unless
      `--reconnect-forever true` is set. Exact-parent retries require a positive
      `max-reconnects` setting or reconnect-forever and preserve the ordinary
      transport budget, so
      repeated parent-recovery failures can exceed `max-reconnects`. Successful
      replay resolves the matching evidence row.
- [ ] Requesting out-of-transaction parent recovery is suspended for the same
      reason: installing the parent from the source can fail on the parent's own
      foreign keys while the backfill is incomplete - installing `guests` for
      `fk_sessions_guest` failed `fk_guests_utm_id` because `utms` was not loaded -
      and a failed recovery is never marked attempted, so the reconnect loop
      re-attempts it at the same coordinate indefinitely and the stream never
      advances. With no recovery request the conflict is an ordinary recorded
      skip. `EXACT_PARENT_RECOVERY_ENABLED` in `row::conflict` gates it and the
      proofs below are `#[ignore]`d together with it; restore both once the
      referenced parent tables are fully loaded.
- [ ] For the two out-of-transaction parent-recovery cases, a persisted `1452` on
      `globalcomix.sessions` naming `fk_sessions_guest` must carry non-empty
      `session_id`, `guest_id`, and `guest_hash`; source `guests` must contain
      exactly one matching row. A persisted `1452` on
      `globalcomix.home_feed_card_slides` naming `fk_hfcs_card` must carry
      positive `slide_id` and `card_id`; source `home_feed_cards` must contain
      exactly one complete row for that card. Each target lookup must return no
      row or exactly one equal row; a no-match lookup gets one exact insert,
      while divergent, colliding, or ambiguous state fails closed. Recovery
      never updates or deletes a parent and never advances the stream checkpoint;
      only the subsequent successful replay can do that.
- [ ] Every other `1452` is recorded and skipped like any other conflict: the row
      is dropped, the evidence is persisted, and the stream advances. A missing
      parent must never hold the stream: an unresolvable row is a skipped row, not
      a stop. The generic in-transaction resolver below is implemented but
      unwired, because it can only succeed when the parent exists in the source
      and is absent from the target, while during backfill the parent is missing
      from both, recovery fails, and a failed recovery aborted the stream instead
      of skipping - `webhooks_requests` -> `sessions` held it down for 45 minutes
      that way. Re-enable it once the referenced parent tables are fully loaded;
      the planned full data resync supplies every row skipped in the meantime.
      The remaining boxes in this section describe that unwired resolver.
- [ ] Every other `1452` is resolved inside the applying transaction, from one
      locked read of the parent taken under `FOR UPDATE`. The constraint identity
      comes from the error text, which names the child table, the constraint, the
      child columns, the parent table, and the referenced columns; only the
      parent's primary key is read from the schema inventory, because the error
      never states it. The locked read selects by that primary key alone, never by
      the full referenced tuple, so an absent parent is distinguishable from a
      parent whose referenced attribute has moved on.
- [ ] An empty locked read is the missing-parent class: install the exact current
      source parent row, then replay the child image unchanged. Source state must
      hold exactly one complete parent owning the referenced identity; absent,
      ambiguous, or mismatched source state fails closed. This class is not gated
      on parent `create_time`, because a generic parent table is not guaranteed to
      have that column.
- [ ] A single locked row whose referenced non-key columns differ is the
      superseded-attribute class: replay the child image with only those derived
      columns fast-forwarded to the locked parent's values, keeping every other
      child column historical. Those columns are maintained by
      `ON UPDATE CASCADE`, so the next replayed parent update writes the same
      values. The referenced primary key columns must match exactly; more than one
      locked row, a shape that disagrees with the error, or no drift at all fails
      closed.
- [ ] Neither in-transaction class updates or deletes a parent row, and a rejected
      resolution is an ordinary durable conflict: roll back, persist evidence, and
      retry from the unchanged checkpoint. Rejection messages carry the
      `superseded ... insert rejected:` marker, because the stream classifies
      fatal against retryable by that prefix.
- [x] Emit parseable `cdc_row_conflict_skipped` output with operation, table,
      source coordinate, and source primary key.
- [x] Replay the same source event into the same identity and increment its
      attempt count; a different source primary key creates a separate identity.
- [x] Keep generated columns out of row writes.
- [x] Derive a stored lowercase ASCII SHA-256 `source_row_identity` from the
      canonical length-prefixed tuple of source identity, schema, table, and
      complete source primary-key JSON. Index that identity with conflict status
      so successful row replay never scans the ledger to find existing debt.
- [x] Look up unresolved source-row debt with the canonical identity plus the
      complete source identity, schema, table, and primary-key JSON as collision
      defenses, stopping after the first exact match. Coordinate and operation
      remain part of `conflict_identity`, but not source-row resolution identity.
- [x] Provide a durable conflict-record schema/library contract containing source
      identity/server/file/start/end, schema/table/operation, source PK,
      duplicate index/owner when available, error code/text, first/last observed
      times, attempt count, unresolved/resolved status, repair run ID, and
      resolution evidence.
- [x] Provide duplicate classification for same-primary, secondary-unique owner
      mismatch, and malformed duplicate errors in the repair library/tests.

## Insert conflict policy boundary

`--insert-conflict-policy` has three values: `error`, `ignore-duplicate`, and
`replace-divergent-pk`. `ignore-duplicate` keeps its equality-only native ROW
behavior: a duplicate continues without ledger evidence only when the target row
fetched by source primary key exactly equals the source row. The narrow
`payments` `INSERT`/MySQL `1644` duplicate-trigger classification above is
independent of `--insert-conflict-policy`: one target row matching all six stable
identity columns is already applied and may checkpoint, while missing,
ambiguous, or divergent identity remains a durable conflict with no checkpoint
advance. `replace-divergent-pk`
is native ROW-only and replaces unequal rows only for a `PRIMARY` duplicate using
a primary-key UPDATE of the source image; it records durable evidence only when
a matching unresolved conflict already exists, then continues so the target
transaction/checkpoint can commit. Successful no-op/replacement events never
create ledger rows. Foreign-key, CHECK, and replacement-update conflicts always
persist evidence and abort. Secondary-unique conflicts do too, except for the
separately specified superseded `globalcomix.users`/`users.name`,
`globalcomix.comics`/`comics.slug`, and `globalcomix.releases` insert proofs,
which can commit only after their specified source/target proofs. The accepted
overwrite risk is
explicit. On the live target-table checkpoint path, the stream locks and validates
the source-scoped predecessor checkpoint in that same target transaction: it
must exist, use the candidate's binlog file, remain before the candidate start,
and not exceed the XID end. The stream then writes the event-end checkpoint,
executes staged resolution SQL, and commits once; only after COMMIT
does it mark the in-process resolution cache committed. A rollback or commit
failure therefore leaves target DML, checkpoint, and ledger resolution unresolved.
Generic statement execution does not gain an unsafe replacement fallback.
Snapshot/catchup writes may use explicit `INSERT IGNORE` independently of the
flag. Normal `sync-table` range repairs use strict batched `INSERT` with
inventory-driven FK parent repair and exact post-write child verification; the
`sync-table --updated-since` path uses an upsert.

## Durable conflict control plane

`cdc.row_conflicts` is bootstrapped by the admin DDL file and is never created by
runtime code. Startup validates the exact columns, nullability/defaults,
ASCII SHA-256 `conflict_identity` primary key, the stored generated
`source_row_identity` expression and `(source_row_identity, status)` index,
unresolved/resolved status CHECK, insert/update guards, and effective privileges
before opening the source stream. The one-time transition runs only while stream
and repair writers are stopped; adding the stored generated column backfills
existing rows from immutable ledger fields before the new index becomes usable.
Trigger metadata is read only through the SQL SECURITY DEFINER, READS SQL DATA
procedure `cdc.row_conflicts_trigger_inventory`; runtime calls that exact
procedure and validates its returned trigger rows before streaming. Admin/resolver
bootstrap separately reviews `SHOW CREATE PROCEDURE` and the direct trigger rows.
The conflict identity is lowercase SHA-256 over an ordered, length-prefixed tuple
of `source_identity`, server ID, binlog file, start position, schema, table,
operation, and the complete source primary-key JSON. The separate source-row
identity uses the same framing over `source_identity`, schema, table, and primary-key
JSON. All identity fields remain stored. Conflict-observation UPSERTs compare
every conflict-identity field; source-row resolution queries use the indexed hash
only to select candidates and retain every unhashed field as an exact collision
defense. A mismatch therefore cannot resolve another source row.

The runtime grant is exact table scope: `SELECT, INSERT, UPDATE` only, plus
`EXECUTE` on the exact inventory procedure. Separately, the reviewed application
schema grant includes `SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP,
INDEX, REFERENCES, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE,
EXECUTE, EVENT, TRIGGER`; application `EXECUTE` is required, while control-plane
or global/admin mutation is rejected. DELETE, ALTER, DROP, other CDC
EXECUTE/schema scopes, schema-wide/global/admin/role/grant-option access is
rejected.
Observations use a guarded UPSERT that increments unresolved attempts but never
downgrades a resolved record. Resolution updates only an unresolved record after
verified source/target equality and requires non-empty repair evidence.

It is forbidden to convert an insert into an update of the secondary-key owner
or to select a target row by the secondary key.

## Live wiring and remaining proof

The structured live stream stages supported constraint-conflict observations
within the source transaction. At the source XID, it finalizes their
source-transaction end coordinates, rolls back the target transaction, then
persists the unresolved observations through the independent durable ledger
before returning the row failure. The failed transaction and later coordinates
are not checkpointed, while the independently persisted evidence survives.
The reconnect loop below covers only the two pinned constraints. Any other
foreign-key conflict never reaches it: it is deferred and resolved inside the
applying transaction under the parent lock, so it carries no reconnect recovery
request and cannot retry outside the transport budget.

For each exact parent-recovery identity, the reconnect loop first verifies that
exact-parent retry is enabled and the error is retryable, then performs strict
source/target parent validation after rollback and durable ledger persistence. A
failed reconciliation remains eligible for another attempt without consuming the
ordinary transport budget; after success, a later retry of the same request skips
parent mutation but still replays from the unchanged checkpoint. The `sessions` path
requires the exact `fk_sessions_guest` scope and `guests` composite identity;
the home-feed path requires `fk_hfcs_card` and
`home_feed_card_slides.card_id` → `home_feed_cards.id`. For `sessions`, the exact
constraint name, ordered child columns (`guest_id`, `guest_hash`), and parent
reference `REFERENCES `guests` (`guest_id`, `guest_hash`) are required; for
home-feed cards, the exact constraint and `card_id` parent reference are
required. Suffix/name substring or alternate-parent matches are ineligible. Each
typed request carries the persisted source transaction coordinate, child primary
key, child identity fields, and child event timestamp. It is reconstructed deterministically from the replayed row image and
persisted conflict identity, not stored in `cdc.row_conflicts`. Recovery source
and target connections set `time_zone='+00:00'` once when each connection is
created, before full parent reads or insertion. Each identity query returns that path's canonical parent columns plus a dedicated
`UNIX_TIMESTAMP(create_time)` helper epoch. That absolute epoch must be no later
than the child event; the session-time-zone-rendered `create_time` text does not
control ordering, and the helper is excluded from insert and exact-row comparison.
Missing or invalid epochs fail closed. A successful reconciliation is recorded
at most once per distinct reconstructed `ExactParentRecovery` value per process
reconnect loop; failed attempts remain eligible for retry. This is not
ledger-identity deduplication. Recovery is admitted with a positive
`max-reconnects` setting or reconnect-forever; with `max_reconnects=0` and
reconnect-forever false, it is
skipped without reading or mutating the recovery target. A later retry after
successful reconciliation skips mutation but still preserves the ordinary
transport budget.
Existing exact target parents are accepted idempotently after process loss;
otherwise one current source parent image is inserted only when the target has
no matching identity. Unsupported, absent, duplicate, colliding, divergent, or
temporally invalid identities, connection failures, and insert failures return a
contextual typed recovery failure; the reconnect loop retries it under
exact-parent policy without replay or checkpoint advance. Recovery emits deterministic
attempted/skipped/succeeded/failed
logs. It never resolves the ledger entry; normal child replay must commit and
checkpoint before the existing resolution path can mark it resolved. Recovery
is not generic FK repair, performs no historical binlog reconstruction, and
requires a durable checkpoint store.
Equal native ROW `INSERT` duplicates are logged and applied without ledger
persistence or rollback; divergent native ROW `INSERT` duplicates follow the
durable conflict path.
Replaying
the same source identity is idempotent: existing conflict evidence is resolved
only after successful target commit/checkpoint, and a different source primary
key remains a distinct identity. Startup validates the ledger schema, guards, trigger inventory, and
exact grants before source replication. `repair-drift` resolves rows only after
its non-mutating Verify phase proves full-scope equality, then records the run ID
plus evidence. The Docker harness `replace-divergent-pk` scenario proves a real ROW transaction's
XID target commit and checkpoint advancement, successful replacement without
creating a ledger row, and a replacement CHECK failure rolling back target DML
without advancing the checkpoint. The harness does not inject a
crash between target commit and process completion; that crash boundary remains
unproven. The `row-conflict-source-row-migration` scenario applies the one-time transition
to an existing populated ledger and proves generated backfill, exact index shape,
and guard immutability. The `row-conflict-indexed-resolution` scenario proves on
real MySQL that the exact unresolved-row lookup selects the source-row/status
index, rejects a hash candidate with different unhashed source identity, resolves
existing evidence only after successful replay commit, and advances the
checkpoint. The `row-conflict-rollback` scenario passes
`--insert-conflict-policy ignore-duplicate` for an equal same-primary-key
`ROW INSERT`, and asserts that the target transaction succeeds, the checkpoint
advances to the event end, and no unresolved ledger row exists for that source
primary key. It then asserts rollback, unchanged checkpoints, and durable
idempotent evidence for a divergent secondary-unique conflict, different
primary-key isolation, and a CHECK conflict. The structured-stream transaction
tests separately assert the same rollback/evidence boundary for a foreign-key
conflict; unit coverage proves exact session/guest and home-feed-card recovery
extraction, epoch-based temporal ordering, canonical-row handling, and
request-value retry ordering,
but not real source/target reads or inserts. The harness's FK
scenarios cover repair ordering and cycle blocking.

- [ ] Schedule recurring repair from unresolved records.
- [ ] Prove the live deployed path and repeated convergence before cutover.

See [Table Sync Repair](table-sync-repair.md) and
[Catchup Workflow](../catchup.md).
