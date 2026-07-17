# Checkpoints

The file checkpoint format remains useful for rehearsals, but live
`stream-binlog` uses the target table `cdc.stream_checkpoint` as its authoritative
resume state.

A live row is scoped to `stream-binlog:<source-identity>`. The source identity
must change when the source incarnation changes. Runtime validates the
pre-created table and source-scoped row; it does not create or repair the control
plane.

## Automatic DDL journal

Automatic DDL is not a separate execute-then-checkpoint shortcut. The journal
(`cdc.ddl_replay_journal`) is distinct from the manual DDL ledger
(`cdc.ddl_events`). For an admitted index event the order is:

1. Validate bootstrap objects, exact grants, the single-writer nonblocking
   `GET_LOCK(SHA2(<lease-name>,256),0)`, and startup barrier. This is a
   single-writer lock only; there is no multi-writer fence, CAS, or fencing token.
2. Capture immutable target pre-state and the parsed/canonical AST. The expected
   post-state is derived from that recorded pre-state plus the translated AST;
   current source metadata is never substituted.
3. Insert one immutable journal row as `prepared`.
4. Execute the admitted DDL.
5. Capture and validate the complete affected target state.
6. Transition `prepared -> applied`.
7. In one target transaction, lock and require the exact predecessor checkpoint,
   transition `applied -> checkpointed`, and save the event-end checkpoint.

The journal permits only `prepared -> applied|blocked` and
`applied -> checkpointed`. Identity, SQL, AST, pre-state, and expected post-state
are immutable. `blocked` and `checkpointed` are terminal.

A crash after `prepared` is never handled by blind re-execution. Reconciliation
can finalize only when the observed target state exactly equals a unique expected
post-state and differs from the recorded pre-state. Observed pre-state, both or
neither states, mixed/unavailable evidence, or any mismatch becomes `blocked`.
The source does not provide a target-binlog receipt, so this is semantic proof
with an irreducible ambiguity boundary.

The earliest `prepared` or `blocked` row for the source identity is a startup
barrier. Later source coordinates cannot overtake it. A barrier is a stop and
requires operator review; it is not a retry hint.

## Manual DDL ledger

Unsupported or incomplete DDL is recorded in `cdc.ddl_events` as `pending` after
earlier DML is flushed. The stream does not execute it and does not advance the
checkpoint. An operator applies and validates the reviewed target change, marks
the same immutable row `resolved`, and restarts. Restart requires byte-for-byte
raw-SQL equality and advances without re-executing the statement.

## Bounded stop semantics

`--stop-position` is an inclusive event-end boundary. The stream dispatches and
checkpoints the event whose `end_log_pos` equals the requested position, then
exits cleanly. A position inside an event, inside an open row transaction, or not
reached before EOF fails explicitly; it never commits a partial transaction.

## Remaining proof gaps

- [ ] Exercise journal/bootstrap validation against the live target and review
      deployment credentials.
- [ ] Prove target schema/data convergence and lag after deployment.
- [ ] Prove GTID persistence/resume; live checkpoints currently store file/position
      with `gtid: null`.
- [ ] Schedule recurring repair from durable unresolved conflicts and prove
      repeated convergence before cutover.
