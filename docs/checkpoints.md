# Checkpoints

The file checkpoint format remains useful for rehearsals, but live
`stream-binlog` uses the target table `cdc.stream_checkpoint` as its authoritative
resume state.

A live row is scoped to `stream-binlog:<source-identity>`. The source identity
must change when the source incarnation changes. Runtime validates the
pre-created table and source-scoped row; it does not create or repair the
control plane.

Unified synchronization stores aggregate stage/table progress in the selected
progress table, `cdc.sync_runs` by default, keyed by `(run_id, stage, table_name)`.
For incomplete row work it also requires the additive `<progress-table>_phases`
table, keyed by the unchanged run ID, table name, and mutation phase. Apply
[`sync-phase-progress-bootstrap.sql`](sync-phase-progress-bootstrap.sql) before a
default run; it creates `cdc.sync_runs_phases` and grants only `SELECT`, `INSERT`,
and `UPDATE` on that table to the deployed sync account. Current runtime also
executes idempotent `CREATE TABLE IF NOT EXISTS` before incomplete row work, so
bootstrap does not remove its existing `CREATE ON cdc.*` requirement. A run with
all selected legacy `rows` records complete bypasses phase-table access; incomplete
legacy cursors never seed phases. Changing the
progress-table option selects different aggregate and phase stores. Resume resolves
endpoints, TLS, current table scope and definitions, chunk size, and parallelism
fresh. Omitted rows remain untouched, completed selected rows skip, and newly
selected tables create missing stages. The legacy `run_spec_json` column is ignored
and never migrated or rewritten. `--run-id-prefix` retains backward-compatible
`sync-v1` generation from serialized invocation/table input; use an exact `--run-id`
for mutable resume because changing that input changes the derived prefix ID.

## Lost-binlog recovery control plane

`recover-lost-binlog` is the availability-first, incident-scoped transition for
one purged-history barrier. It is not a generic checkpoint setter and does not
claim that the skipped source interval was replayed.

The CLI reads operator JSON containing the exact old checkpoint and exact
`cdc.ddl_replay_journal` barrier, including source identity, file, start/end
positions, and raw SQL. It rejects a configured source/checkpoint identity
mismatch. Before preparing recovery it computes and records the current complete source
scope hash for that attempt and rejects any non-InnoDB source table. Recovery
synchronization covers every current source-scope table even when target-only
base tables exist. An explicitly
supplied scope hash must match that attempt's current source inventory; an omitted
hash is filled from the current source inventory and recorded as evidence.

The stream lease is acquired before transition. The source binlog coordinate is
captured with ordinary non-locking reads. Recovery does not execute `FLUSH TABLES
WITH READ LOCK`, `UNLOCK TABLES`, or `LOCK TABLES`, does not require `RELOAD`, and
does not keep a cross-table repeatable-read snapshot open. Source schema and row
reconciliation use normally committed reads. Source commits after the captured
coordinate remain eligible for stream replay after the checkpoint advances.

A prepared immutable row is inserted into `cdc.stream_recovery_records` with the
old state, captured coordinate, source identity, that attempt's scope hash,
operator, reason, and source-only preparation evidence. The captured source
evidence drives one unified staged sync over every source table under the exact
recovery ID. Constraint-preserving prerequisite schema convergence, target component-WRITE-
locked source-authoritative phases, durable aggregate `cdc.sync_runs` progress and
additive phase cursors, and final constraints define successful reconciliation. Proof requires exactly one
complete progress result for every expected source table, with no missing,
unexpected, duplicate, incomplete, or wrong-run rows. Before the final target
transaction, recovery rechecks only the captured source scope hash; it does not
re-inventory the target or run a post-write drift scan. A separately authorized
replacement may, in the same target transaction, lock the exact prepared owner,
mark it `abandoned` with server-generated timestamp/evidence, and insert the
replacement `prepared` row for the same exact checkpoint, barrier, and source
identity. The replacement records its own scope hash; it need not equal the
abandoned scope. All old identity, scope, and prepared evidence remain intact.
Only after exact unified progress proof and unchanged source scope does one
target transaction revalidate the exact checkpoint, barrier, source identity,
and prepared recovery row, update `cdc.stream_checkpoint`, and mark the recovery
`committed`. Abandoned history remains durable and does not suppress the journal
barrier; active-barrier selection excludes only exact `committed` or `verified`
ownership, which is terminal. Any failed validation or commit rolls back both
replacement steps. Duplicate recovery IDs and non-advancing coordinates are
refused.

Bootstrap `cdc.stream_recovery_records`, its immutability guards, and its
trigger-inventory procedure with `docs/stream-recovery-records-bootstrap.sql`
while stream writers are stopped. This control plane is required by every stream
startup: the stream validates the DDL journal contract through its inventory
procedure and requires access to the recovery table. The recovery inventory
procedure supports bootstrap/operator inspection. Missing required objects or
grants fail startup; there is no legacy fallback.

Startup selects unresolved journal barriers by a literal source identity prefix
followed by `#server-id=` and any server ID. The SQL `LIKE` predicate uses `=` as
its escape character, so the literal separator is encoded as `#server-id==%`.
A preserved historical journal barrier is excluded only when a recovery row has
status `committed` or `verified` and exactly matches its source identity, file,
start/end coordinates, and raw-SQL hash. Prepared or abandoned recoveries never
suppress a barrier.

This documentation records the control-plane contract only; production
execution, restart health, and post-transition `verified` evidence remain open.

### Prepared recovery resume and activation

`resume-lost-binlog` continues only an existing `prepared` recovery matching its
original authorization and unchanged old checkpoint/barrier. It retains the
original captured coordinate, recovery ID, immutable evidence, and
`cdc.sync_runs` completed/partial progress. It acquires the same
`cdc-stream:<target_database>` lease as live streaming and fresh recovery.
Current source scope/schema must match the prepared evidence; the original
binlog file/position must remain available before work and immediately before
commit. Complete exact-scope progress and the existing atomic CAS remain required.

`activate-lost-binlog` is narrower: it accepts only that authorized existing
`prepared` record when its exact source scope and retained prepared boundary
still match. It read-only loads prerequisite and `Rows` phase completion and
requires every expected table complete. It does not run DDL, read or write
source/target table data, rescan rows, capture a new boundary, change source
configuration, or alter final-constraint progress. It uses the same atomic
checkpoint/recovery-record commit as full recovery, with evidence stating
`schema_converged=false` and `final_constraints_deferred=true`; final FK
convergence remains background work.

Resume never recaptures a boundary, resets progress, abandons the record, or
substitutes a fresh scan. Activation likewise cannot bypass missing, terminal,
mismatched, changed-scope, incomplete-progress, or expired-boundary state.
`recover-lost-binlog` remains fresh-only and rejects an already-used ID.
Post-capture changes to completed tables or scanned prefixes are applied later
by streaming from the retained original boundary.

## Automatic DDL journal

The event handler represents DDL in the durable journal
(`cdc.ddl_replay_journal`). Automatic admission currently covers the narrow
slices described in the [DDL transformation spec](specs/ddl-transformation.md):
explicitly named, unqualified, visible, non-unique secondary BTREE
`CREATE INDEX`/`DROP INDEX`; fixture and exact production `CREATE TABLE` forms;
the exact `assistant_reply_reports` convergence recovery; production-observed
`ALTER TABLE` add/drop/rename forms; and the identity-scoped exact procedure
`CREATE` plus exact generic/plain `DROP` forms. The production-observed
unqualified multi-clause `ALTER TABLE` form with `ADD COLUMN` under the exact
unquoted type grammar
`VARCHAR(positive canonical decimal length)`, `DATETIME`, `SMALLINT UNSIGNED`, or
`FLOAT UNSIGNED`, the observed `NULL` or `NOT NULL`, `DEFAULT NULL` or
`DEFAULT 0`, `COMMENT`, and `AFTER` options, and named composite `ADD KEY`,
MariaDB-syntax `ADD INDEX` normalized to the same AST, or `ADD UNIQUE KEY`
clauses. Multiple admitted clauses render in source order as
deterministic MySQL 8 SQL; source `ADD INDEX` emits as target `ADD KEY`. One
exact `releases` index-rebuild shape also admits `DROP INDEX idx_downloads_sort`,
the observed eight-part replacement index with `published_time DESC`,
`comic_id ASC`, and `id ASC`, then `ALGORITHM=INPLACE, LOCK=NONE`; every
variation remains `translation_pending`. The slice also admits `DROP COLUMN IF EXISTS` with
ASCII-case-insensitive target matching, one emitted drop per matched target spelling,
and absent or repeated case-variant no-ops; and the production-observed unqualified
multi-clause `ALTER TABLE ... RENAME COLUMN IF EXISTS ...` form. For the
implemented ALTER slice, expected post-state is derived from fenced target
pre-state plus the event AST; historical replay does not require a live source
head at the event coordinate. The ALTER `ADD COLUMN` slice admits only the exact
unquoted type grammar `VARCHAR(positive canonical decimal length)`, `DATETIME`,
`SMALLINT UNSIGNED`, or `FLOAT UNSIGNED`; quoted type keywords, quoted `VARCHAR`
lengths, and quoted `UNSIGNED` forms are unsupported, as are `DATETIME` precision,
`SMALLINT` display width, and `FLOAT` parameters. Unsupported defaults, options,
comments, and clauses enter `translation_pending` with no target DDL or checkpoint
advance.

For an admitted event, the order is:

1. Validate bootstrap objects, exact grants, the single-writer nonblocking
   `GET_LOCK(SHA2(<lease-name>,256),0)`, and the startup barrier. This is a
   single-writer lock only; there is no multi-writer fence, CAS, or fencing token.
2. Classify the source DDL. If its translator is unavailable, flush earlier
   grouped DML and insert `translation_pending` with
   `transformation_version='translator-unavailable'`, `generated_sql=NULL`, and
   empty canonical/pre/post evidence. The event-end checkpoint does not advance.
3. When translator code is available, reprocess the same event. Capture the
   fenced target pre-state and canonical AST, derive the expected post-state by
   applying the event AST to that pre-state, and promote that same journal row
   exactly once to `prepared`, filling the transformation version and evidence.
   For the implemented production ALTER slice, this requires no live source head
   at the historical event coordinate. No operator-authored SQL or status change
   is involved.
4. Execute the generated MySQL SQL (or the proven no-op), capture and validate
   the complete affected target state, then transition `prepared -> applied`.
5. In one target transaction, lock and require the exact predecessor checkpoint,
   transition `applied -> checkpointed`, and save the event-end checkpoint.

The journal state machine is:

```text
translation_pending -> prepared -> applied -> checkpointed
prepared -> blocked
```

Identity and source SQL are immutable. Once evidence exists,
transformation version, generated SQL, canonical AST, pre-state, and expected
post-state are immutable. A proven no-op stores `generated_sql = NULL`; otherwise
that field is the exact transformed SQL executed.

`translation_pending`, `prepared`, and `blocked` are startup barriers. Later
source coordinates cannot overtake them. Translation failure and evidence-capture
failure use the same `translation_pending` barrier. A translator upgrade may
promote that row automatically; it is not a retry hint or an operator-resolution
state. `blocked` remains a hard review barrier for postcondition mismatch,
ambiguous crash evidence, or other unrecoverable proof failure.

A crash after `prepared` is never handled by blind re-execution. Reconciliation
can finalize only when the observed target state exactly equals a unique expected
post-state and differs from the recorded pre-state. Observed pre-state, both or
neither states, mixed/unavailable proof, or any mismatch becomes `blocked`. The
source does not provide a target-binlog receipt, so this is semantic proof with
an irreducible ambiguity boundary.

## Production ALTER proof

The disposable real MariaDB 11.4/MySQL 8.0 `production-alter-table` scenario
replays five supported ALTER events, checks column/comment/non-unique and unique
index parity, duplicate rejection, translated column removal, and an absent-column
no-op, and requires all five journal rows plus the final supported-event source
checkpoint to be `checkpointed`. It then proves
an unsupported unique-prefix option remains `translation_pending` without target
execution or checkpoint advancement. Targeted unit and structured-stream tests
cover the exact `releases` directional-index admission, but its disposable
MariaDB/MySQL harness extension is currently blocked by a pre-existing scenario
timeout. No deployment, recovery, or live-stream success follows from that unit
proof. It proves only the implemented observed ALTER slice, not full ALTER TABLE
coverage, a full matrix, or deployment readiness.

## Bootstrap

This schema is pre-production. Run the fresh control-plane and journal bootstrap
files while the stream is stopped. For an existing populated
`cdc.row_conflicts` table, first run
`docs/row-conflicts-source-row-identity-migration.sql` once with stream and repair
writers stopped, before startup validation. Obsolete development migrations are
deleted instead of maintained as compatibility paths.

## Remaining proof gaps

- [ ] Bootstrap and execute the lost-binlog recovery control plane against the
      intended target; no production recovery is claimed.
- [ ] Record post-transition recovery `verified` evidence with zero unresolved
      schema/data drift.
- [ ] Exercise journal/bootstrap validation against the live target and review
      deployment credentials.
- [ ] Prove target schema/data convergence and lag after deployment.
- [ ] Prove GTID persistence/resume; live checkpoints currently store file/position
      with `gtid: null`.
- [ ] Schedule recurring repair from durable unresolved conflicts before
      cutover.

## Retired manual ledger

The manual ledger is absent from runtime, configuration, bootstrap, grants, and
harness behavior. Do not use manual SQL/status edits to clear a journal barrier;
unsupported syntax must remain in the automatic journal until translator support
is deployed.
