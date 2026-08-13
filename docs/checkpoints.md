# Checkpoints

The file checkpoint format remains useful for rehearsals, but live
`stream-binlog` uses the target table `cdc.stream_checkpoint` as its authoritative
resume state.

A live row is scoped to `stream-binlog:<source-identity>`. The source identity
must change when the source incarnation changes. Runtime validates the
pre-created table and source-scoped row; it does not create or repair the
control plane.

## Lost-binlog recovery control plane

`recover-lost-binlog` is the availability-first, incident-scoped transition for
one purged-history barrier. It is not a generic checkpoint setter and does not
claim that the skipped source interval was replayed.

The CLI reads operator JSON containing the exact old checkpoint and exact
`cdc.ddl_replay_journal` barrier, including source identity, file, start/end
positions, and raw SQL. It rejects a configured source/checkpoint identity
mismatch. Before preparing recovery it computes the current complete source
scope hash and rejects any non-InnoDB source table. Recovery data repair covers
every current source-scope table even when target-only base tables exist; the
generic `repair-drift` contract remains strict. An explicitly supplied scope
hash must match; an omitted hash is filled from the current source inventory and
recorded as evidence.

The stream lease is acquired before transition. A dedicated MariaDB connection
briefly executes `FLUSH TABLES WITH READ LOCK` while the snapshot connection
opens one `REPEATABLE READ` consistent snapshot and executes `SHOW MASTER STATUS`
on that same connection to capture its binlog coordinate. The write fence is
then released, but that one source transaction remains open for the complete
configured-scope insert, update, delete, and verification phases. Independent
live reads are not recovery evidence.

A prepared immutable row is inserted into `cdc.stream_recovery_records` with the
old state, captured coordinate, source identity, scope hash, operator, reason,
and preparation evidence. Recovery-only schema convergence runs after
source-scoped data repair, drops target-only base tables child-before-parent
with normal foreign-key enforcement, and fails closed on cycles or source-table
references to target-only parents. The final target table inventory must exactly
match source. Only after zero skipped/unsupported tables and successful
schema/data proof does one target transaction revalidate the exact checkpoint,
barrier, source/scope identity, and prepared recovery row, update
`cdc.stream_checkpoint`, and mark the recovery `committed`. The historical
journal row remains intact. Active-barrier selection excludes only the exact
committed source/file/start/end/raw-SQL hash. Any failed validation or commit
rolls back; prepared recovery IDs are immutable and non-resumable, so a prepared
failure requires a separately authorized new recovery ID. Duplicate recovery IDs
and non-advancing coordinates are refused.

Bootstrap `cdc.stream_recovery_records` and its immutability guards with
`docs/stream-recovery-records-bootstrap.sql` while stream writers are stopped.
This documentation records the control-plane contract only; production
execution, restart health, and post-transition `verified` evidence remain open.

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
deterministic MySQL 8 SQL; source `ADD INDEX` emits as target `ADD KEY`. The
slice also admits `DROP COLUMN IF EXISTS` with
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
execution or checkpoint advancement. It proves only
the implemented observed ALTER slice, not full ALTER TABLE coverage, a full matrix,
or deployment readiness.

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
