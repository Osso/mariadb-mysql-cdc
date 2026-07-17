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
(`cdc.ddl_replay_journal`). For an admitted event, the order is:

1. Validate bootstrap objects, exact grants, the single-writer nonblocking
   `GET_LOCK(SHA2(<lease-name>,256),0)`, and the startup barrier. This is a
   single-writer lock only; there is no multi-writer fence, CAS, or fencing token.
2. Classify the source DDL. If its translator is unavailable, flush earlier
   grouped DML and insert `translation_pending` with
   `transformation_version='translator-unavailable'`, `generated_sql=NULL`, and
   empty canonical/pre/post evidence. The event-end checkpoint does not advance.
3. When translator code is available, reprocess the same event. Capture the
   immutable target pre-state and canonical AST, derive the expected post-state,
   and promote that same journal row exactly once to `prepared`, filling the
   transformation version and evidence. No operator-authored SQL or status
   change is involved.
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

## Legacy test/config artifact

The current atomic code slice leaves `src/live/ddl_ledger.rs` behind
`#[cfg(test)]`, along with legacy `ddl_ledger_table` configuration/parser symbols
and tests. These are not a supported DDL workflow, but cleanup is incomplete:
config/bootstrap/grants and harness/test dependencies remain open. Do not use
manual SQL/status edits to clear a journal barrier.
