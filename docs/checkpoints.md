# Checkpoints

The file checkpoint format remains useful for rehearsals, but live
`stream-binlog` uses the target table `cdc.stream_checkpoint` as its authoritative
resume state.

A live row is scoped to `stream-binlog:<source-identity>`. The source identity
must change when the source incarnation changes. Runtime validates the
pre-created table and source-scoped row; it does not create or repair the
control plane.

## Automatic DDL journal

The event handler represents DDL in the durable journal
(`cdc.ddl_replay_journal`). Automatic admission currently covers three narrow
slices: explicitly named, unqualified, visible, non-unique secondary BTREE
`CREATE INDEX`/`DROP INDEX` with complete parsed options and no FK dependency;
the production-observed unqualified multi-clause `ALTER TABLE` form with
`ADD COLUMN` under the exact unquoted type grammar
`VARCHAR(positive canonical decimal length)`, `DATETIME`, or `SMALLINT UNSIGNED`,
the observed `DEFAULT NULL`, `NULL`, `COMMENT`, and `AFTER` options, and named
composite `ADD KEY` or `ADD UNIQUE KEY`, plus `DROP COLUMN IF EXISTS` with
ASCII-case-insensitive target matching, one emitted drop per matched target spelling,
and absent or repeated case-variant no-ops; and the production-observed unqualified
multi-clause `ALTER TABLE ... RENAME COLUMN IF EXISTS ...` form. For the
implemented ALTER slice, expected post-state is derived from fenced target
pre-state plus the event AST; historical replay does not require a live source
head at the event coordinate. The ALTER `ADD COLUMN` slice admits only the exact
unquoted type grammar `VARCHAR(positive canonical decimal length)`, `DATETIME`, or
`SMALLINT UNSIGNED`; quoted type keywords, quoted `VARCHAR` lengths, and quoted
`UNSIGNED` forms are unsupported, as are `DATETIME` precision and `SMALLINT` display
width. Such variants enter `translation_pending` with no target DDL or checkpoint
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
files while the stream is stopped. Obsolete development migrations are deleted
instead of maintained as compatibility paths.

## Remaining proof gaps

- [ ] Exercise journal/bootstrap validation against the live target and review
      deployment credentials.
- [ ] Prove target schema/data convergence and lag after deployment.
- [ ] Prove GTID persistence/resume; live checkpoints currently store file/position
      with `gtid: null`.
- [ ] Schedule recurring repair from durable unresolved conflicts and prove
      repeated convergence before cutover.

## Retired manual ledger

The manual ledger is absent from runtime, configuration, bootstrap, grants, and
harness behavior. Do not use manual SQL/status edits to clear a journal barrier;
unsupported syntax must remain in the automatic journal until translator support
is deployed.
