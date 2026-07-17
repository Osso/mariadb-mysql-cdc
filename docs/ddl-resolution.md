# DDL Resolution Runbook

The event handler uses one durable DDL control plane:
`cdc.ddl_replay_journal`. Unsupported DDL never routes through a manual ledger.

Automatic admission currently covers strict named, unqualified, visible,
non-unique secondary BTREE `CREATE INDEX`/`DROP INDEX` with complete parsed
metadata and no FK dependency; the production-observed unqualified multi-clause
`ALTER TABLE` form with `ADD COLUMN` for `VARCHAR(length)`, `DATETIME`, or
`SMALLINT UNSIGNED`, the observed `DEFAULT NULL`, `NULL`, `COMMENT`, and `AFTER`
options, named composite `ADD KEY` or `ADD UNIQUE KEY`, and `DROP COLUMN IF EXISTS`
with ASCII-case-insensitive target matching, one emitted drop per matched target spelling, and absent or repeated case-variant no-ops; plus the existing
`ALTER TABLE ... RENAME COLUMN IF EXISTS ...` translator slice. Every other DDL
form enters the same journal as `translation_pending`; no operator-authored
target SQL is accepted as a resolution path.

The stream does not create or repair control-plane objects. Bootstrap must run
with admin/resolver credentials while the stream is stopped:

```bash
mariadb --defaults-extra-file=/path/admin.cnf < docs/ddl-control-plane-bootstrap.sql
mariadb --defaults-extra-file=/path/admin.cnf < docs/ddl-replay-journal-bootstrap.sql
```

The contract provisions checkpoint, row-conflict, and automatic DDL journal
objects with exact control-plane scopes and journal/row-conflict trigger-inventory
procedures.

This schema is still pre-production. Fresh bootstrap is the only supported
schema contract; obsolete development migrations are deleted rather than
maintained as upgrade paths.

## Startup/bootstrap validation boundary

Bootstrap and startup validate external administrative state once, before source
replication: journal/checkpoint/conflict columns, keys, checks, guards,
trigger-inventory procedure call results, effective grants, and the single-writer
`GET_LOCK` prerequisite. Admin/resolver bootstrap separately reviews
`SHOW CREATE PROCEDURE` and direct trigger rows. A mismatch is deployment drift
and fails fast. Runtime does not create or repair that state.

Binlog DDL is untrusted source input and is classified per event against the
admission policy. After translation, CDC-generated SQL is trusted internal
program behavior. Event handling executes known internal operations, performs
only event-specific state/evidence checks, and surfaces database errors. It does
not rerun effective-grant policy validation, query `SHOW GRANTS`, or maintain a
second grant/control-plane allowlist.

The removed manual ledger is absent from runtime, configuration, startup
validation, bootstrap, grants, and harness behavior.

## Automatic journal state machine

The expected journal row is:

```text
(source_identity, source_server_id, binlog_file, event_start_position,
 event_end_position, schema_name, raw_sql, transformation_version, generated_sql,
 canonical_ast, pre_state, expected_post_state, status)
```

The allowed transitions are:

```text
translation_pending -> prepared -> applied -> checkpointed
prepared -> blocked
```

### Translator unavailable

When an event has no available translator, the stream flushes earlier grouped
DML and inserts one immutable journal row containing the exact source identity,
coordinates, schema, and raw SQL. It stores:

- `status = 'translation_pending'`;
- `transformation_version = 'translator-unavailable'`;
- `generated_sql = NULL`;
- empty canonical AST, pre-state, and expected post-state evidence.

The event-end checkpoint does not advance. The earliest
`translation_pending`, `prepared`, or `blocked` row for the source identity is a
startup barrier, so later source coordinates cannot overtake it.

### Translator becomes available

Reprocess the same source event. The stream captures a fenced target pre-state
and the canonical AST, derives the expected post-state by applying that AST to
the pre-state, and promotes the existing `translation_pending` row exactly once
to `prepared`. For the implemented production ALTER slice, this derivation is
purely target-pre-state-plus-event-AST; historical replay does not require a live
source snapshot or source head at the event coordinate. The promotion fills
immutable transformation evidence. It then executes the generated MySQL SQL, or
records a proven no-op with `generated_sql = NULL`, validates the complete
affected target state, marks `applied`, and atomically checkpoints the event.

No operator-authored target SQL, `resolution_note`, or manual status transition
is part of this flow.

### Production ALTER proof

The real MariaDB 11.4/MySQL 8.0 harness scenario `production-alter-table`
replays five source ALTER events. It verifies `VARCHAR(64)`, `DATETIME`, and
`SMALLINT UNSIGNED` column metadata, comments and placement, named composite
non-unique and unique BTREE indexes, duplicate-row rejection parity, translated
column removal and its absent-column no-op, five `checkpointed` journal rows with transformation evidence/version, and the final
stream checkpoint. A neighboring unique-prefix option remains
`translation_pending` with no target index and no checkpoint advancement. This is implemented-slice proof only, not full ALTER TABLE coverage,
a full compatibility matrix, or deployment proof.

## Transformation/evidence failure

Translation failure and evidence-capture failure use the same
`translation_pending` journal insert and no-overtake barrier. Retry only by
reprocessing the same source event after the required translator/evidence code is
available. Do not create a second row or edit sentinel/evidence fields manually.

### Crash after preparation

A restart never blindly re-executes a `prepared` DDL. Reconciliation can finalize
only when observed target state exactly equals a unique expected post-state and
differs from the recorded pre-state. Observed pre-state, both/neither states,
mixed/unavailable proof, or any mismatch transitions the row to `blocked` and
halts progress. Target-binlog receipt is unavailable; this is semantic proof
only.

### Journal inspection

```sql
SELECT
    source_identity,
    source_server_id,
    binlog_file,
    event_start_position,
    event_end_position,
    schema_name,
    raw_sql,
    transformation_version,
    generated_sql,
    canonical_ast,
    pre_state,
    expected_post_state,
    status,
    created_at,
    updated_at
FROM cdc.ddl_replay_journal
WHERE status IN ('translation_pending', 'prepared', 'blocked')
ORDER BY binlog_file, event_start_position;
```

Treat every returned row as a replication and cutover blocker. Inspect the
immutable evidence and target state. Do not apply an operator-authored SQL
statement or mutate the journal to bypass the barrier. A `translation_pending`
row is cleared only by translator code becoming available and automatic
promotion; a `blocked` row requires an explicitly reviewed product/operations
resolution outside this runtime slice.

## Runtime grant contract

Required control-plane scopes are separate from the application-schema grant:

- global `USAGE` only;
- `SELECT, INSERT, UPDATE` on `cdc.stream_checkpoint`;
- `SELECT, INSERT, UPDATE` on `cdc.row_conflicts`;
- `SELECT, INSERT, UPDATE` on `cdc.ddl_replay_journal`;
- `EXECUTE` only on the exact definer-safe
  `cdc.row_conflicts_trigger_inventory` and
  `cdc.ddl_replay_journal_trigger_inventory` procedures.

Reject control-plane/global/admin mutation, `ALL`, `GRANT OPTION`, `PROXY`,
roles, broad `cdc.*`, and row-conflict `DELETE`, `ALTER`, or `DROP` privileges.
The startup/bootstrap validator fails before source streaming when the tables,
guards, constraints, procedures, or effective exact grant is missing or widened.
Application-schema privileges remain a separate reviewed bootstrap contract.
Runtime calls the exact inventory procedures during startup validation; it does
not rerun grant policy validation during event handling.

## Monitoring and bounded stops

A bounded stream uses an inclusive event-end stop position. It dispatches and
checkpoints the event whose `end_log_pos` equals `--stop-position`, then exits.
A stop inside an event or open row transaction, or a stop not reached before EOF,
fails explicitly rather than partially applying a transaction.

## Unchecked gates

- [ ] Real-MySQL bootstrap/schema/guard/routine/grant validation.
- [ ] Process crash after prepare, apply, and transactional checkpoint transition.
- [ ] Qualifier/comment/ANSI_QUOTES/incomplete-form integration matrix.
- [ ] Recurring conflict-to-repair scheduling and live convergence proof; the
      durable observation ledger and FK-aware phased repair are wired and covered
      by the Docker harness.

## Retired manual ledger

The manual ledger is absent from runtime, configuration, bootstrap, grants,
harness behavior, and tests. It must not be recreated or used to provision,
grant, or clear a production DDL barrier.
