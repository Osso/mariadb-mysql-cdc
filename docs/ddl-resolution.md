# DDL Resolution Runbook

The event handler uses one durable DDL control plane:
`cdc.ddl_replay_journal`. Unsupported DDL never routes through a manual ledger.

Automatic admission currently covers strict named, unqualified, visible,
non-unique secondary BTREE `CREATE INDEX`/`DROP INDEX` with complete parsed
metadata and no FK dependency; the production-observed unqualified multi-clause
`ALTER TABLE` form with `ADD COLUMN` under the exact unquoted type grammar
`VARCHAR(positive canonical decimal length)`, `DATETIME`, `SMALLINT UNSIGNED`, or
`FLOAT UNSIGNED`; quoted type keywords, quoted `VARCHAR` lengths, and quoted
`UNSIGNED` forms are rejected, as are `DATETIME` precision, `SMALLINT` display
width, and `FLOAT` parameters. The observed `NULL` or `NOT NULL`, `DEFAULT NULL`
or `DEFAULT 0`, `COMMENT`, and `AFTER` options, named composite `ADD KEY`,
MariaDB-syntax `ADD INDEX` normalized to the same AST, or `ADD UNIQUE KEY`
clauses. Multiple admitted clauses render in source order as deterministic MySQL
8 SQL; source `ADD INDEX` emits as target `ADD KEY`. The slice also admits
`DROP COLUMN IF EXISTS`
with ASCII-case-insensitive target matching, one emitted drop per matched target spelling, and absent or repeated case-variant no-ops; the source-only
`CREATE PROCEDURE` statements matching either private exact hash for the exact
routine identity `apply_release_move_purchase_repair`; public documentation
omits raw production procedure bodies, `DEFINER` hosts, and event coordinates;
the generic exact
unqualified, unquoted `DROP PROCEDURE IF EXISTS <identifier>` form; and the
exact unqualified, unquoted plain `DROP PROCEDURE apply_release_move_purchase_repair`
form; plus the existing `ALTER TABLE ... RENAME COLUMN IF EXISTS ...` translator
slice. The source-only CREATE admission occurs before generic
qualified-identifier rejection because the admitted statement contains qualified
tokens. Every other body, name, qualified, quoted, commented,
other plain-name, or routine DDL variant remains unsupported. The two admitted
source bodies are tracked as exact fixtures at
`fixtures/ddl/create-apply-release-move-purchase-repair.sql` and
`fixtures/ddl/create-apply-release-move-purchase-repair-95.sql`; adding a
comment or changing any body text changes the hash and remains rejected.
Every other DDL form enters the same journal as `translation_pending`; no operator-authored
target SQL is accepted as a resolution path.

### Exact production ALTER recovery target

The active recovery target is the exact source event at
`mysqld-bin.002778:750897987-750898224`. The event is 150 raw bytes with
CRLF line endings and SHA-256
`ea9f789b158dca0146715bafe9f2712b5945b9c6626411b382347e60e52eb85f`:

```sql
-- The serve-time blacklist check resolves a blacklisted artist's imprints.
ALTER TABLE `artists_imprints`
    ADD KEY `idx_artist_id` (`artist_id`)
```

Admission is limited to this otherwise-supported ALTER preceded by exactly one
ordinary MySQL `-- ` line comment. Embedded comments, executable/version comments,
optimizer hints, and all other leading comment forms remain `translation_pending`
barriers with no target execution or checkpoint advance.

The stream does not create or repair control-plane objects. Bootstrap must run
with admin/resolver credentials while the stream is stopped:

```bash
mariadb --defaults-extra-file=/path/admin.cnf < docs/ddl-control-plane-bootstrap.sql
mariadb --defaults-extra-file=/path/admin.cnf < docs/ddl-replay-journal-bootstrap.sql
```

The contract provisions checkpoint, historical row-conflict, and automatic DDL
journal objects. Live stream startup validates only checkpoint and DDL-journal
state; row-conflict objects remain independent evidence storage.

Before deploying the independent-ledger stream against an account provisioned by
the older contract, run `docs/live-stream-runtime-grants-migration-20260818.sql`
with target admin credentials. It revokes only obsolete `cdc_stream` access to
`cdc.row_conflicts`, its inventory procedure, and `cdc.table_sync_runs`; it does
not delete historical objects or resolver access.

For an existing populated `cdc.row_conflicts` table, run
`docs/row-conflicts-source-row-identity-migration.sql` once with stream and repair
writers stopped, before startup validation. Fresh installations use the bootstrap
file directly. This supported transition adds the generated source-row identity
and lookup index; obsolete development migrations are still deleted rather than
maintained as compatibility paths.

## Startup/bootstrap validation boundary

Bootstrap and startup validate live external administrative state once, before
source replication: journal/checkpoint columns, keys, checks, guards, the DDL
journal trigger-inventory procedure call result, effective stream grants, and the
single-writer `GET_LOCK` prerequisite. Independent conflict-ledger state is not a
live startup dependency. Admin/resolver bootstrap separately reviews its objects.
A live-contract mismatch is deployment drift and fails fast. Runtime does not
create or repair that state.

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
startup barrier, so later source coordinates cannot overtake it. Once the
journal barrier is durable, the live reconnect loop retries the same source
coordinate in-process indefinitely without consuming the ordinary transport
retry budget. It does not skip the event, advance the checkpoint, or execute raw
source SQL.

### Translator becomes available

Reprocess the same source event. The stream captures a fenced target pre-state
and the canonical AST, derives the expected post-state by applying that AST to
the pre-state, and promotes the existing `translation_pending` row exactly once
to `prepared`. For the implemented production ALTER slice, this derivation is
purely target-pre-state-plus-event-AST; historical replay does not require a live
source snapshot or source head at the event coordinate. For the identity-scoped source-only procedure CREATE, target evidence must prove
the routine is absent before and after capture; it executes no target SQL and
records `generated_sql = NULL`. Identity/header admission precedes the
qualification bypass. For either admitted procedure DROP form, target-local
routine pre/post evidence determines whether to emit the quoted target procedure
name or record a proven no-op. The promotion fills immutable transformation evidence. It then executes the generated MySQL SQL,
or records a proven no-op with `generated_sql = NULL`, validates the complete
affected target state, marks `applied`, and atomically checkpoints the event. The
admitted CREATE bodies are never executed: ROW/FULL effects replicate only through
subsequent source events in source order.

No operator-authored target SQL, `resolution_note`, or manual status transition
is part of this flow. An existing `translation_pending` row for the exact
procedure event is promoted automatically; no replacement journal row is
created.

### Exact `assistant_reply_reports` CREATE convergence recovery

The exact production `assistant_reply_reports` CREATE event has a bounded
convergence recovery, not generic `CREATE TABLE` support. Before retrying its
barrier, an operator must provision the target table out of band from the
recorded source definition; the CDC runtime never executes this CREATE. Replay
then admits only the exact raw-event hash, fences a stable current source
inventory, and requires complete equality of the source and target table,
indexes, and foreign-key metadata. A successful equality proof records a
proven no-op (`generated_sql = NULL`) and advances through the normal journal
and checkpoint sequence. A changed statement, absent target, moving source
fence, or semantic mismatch remains `translation_pending` with no checkpoint
advance. Do not clear the barrier with operator-authored SQL or a manual journal
status change.

### Identity-scoped source-only CREATE PROCEDURE

The source-only `CREATE PROCEDURE` form is admitted only when the complete
statement matches one of two recorded hashes for unqualified routine identity
`apply_release_move_purchase_repair`. The exact admitted bodies are the two
tracked DDL fixtures named above. Admission is exact-statement scoped, not
generalized procedure grammar. Qualified tokens are allowed only after this
admission. Target evidence must prove the routine absent
before and after capture; a present target procedure fails closed. The target
receives no CREATE, the body never runs, and later source ROW/FULL events carry
any data changes in source order. Any existing `translation_pending` row for an
admitted event is promoted automatically; no operator SQL or replacement journal
row is used. Every other body, name, and routine DDL remains
`translation_pending`.

### Procedure DROP proof

Two routine-drop forms are admitted. The generic form is exact unqualified,
unquoted `DROP PROCEDURE IF EXISTS <identifier>`. The additional plain form is
only exact unqualified, unquoted `DROP PROCEDURE apply_release_move_purchase_repair`.
Target-local routine inventory determines the operation: an existing target
routine emits deterministic MySQL `DROP PROCEDURE` with the target inventory
spelling backtick-quoted; an absent routine emits no target SQL as a proven
no-op. Qualified, quoted, commented, and other plain-name forms remain
`translation_pending` barriers with no target execution or checkpoint advance.

### Exact DROP PROCEDURE IF EXISTS proof

The generic routine-drop form is exact unqualified, unquoted
`DROP PROCEDURE IF EXISTS <identifier>`, with no comments and an optional
semicolon. Target-local routine inventory is the evidence source. When the named
procedure exists, the translator emits deterministic MySQL `DROP PROCEDURE`
with the target name backtick-quoted, using the target inventory spelling. When
it is absent, it emits no target SQL and records the expected routine state as
absent, which is a proven no-op.

The additional plain routine-drop form is exact unqualified, unquoted
`DROP PROCEDURE apply_release_move_purchase_repair`, without `IF EXISTS`.
Target-local routine inventory determines the same deterministic drop versus
proven no-op. Qualified, quoted, commented, and other plain-name variants remain
`translation_pending` barriers with no target execution or checkpoint advance.

### Production ALTER proof

The real MariaDB 11.4/MySQL 8.0 harness scenario `production-alter-table`
replays five source ALTER events. It verifies `VARCHAR(64)`, `DATETIME`, and
`SMALLINT UNSIGNED` column metadata, comments and placement, named composite
non-unique and unique BTREE indexes, duplicate-row rejection parity, translated
column removal and its absent-column no-op, five `checkpointed` journal rows with transformation evidence/version, and the final
stream checkpoint. Focused parser and structured-stream replay tests cover the
production `FLOAT UNSIGNED NOT NULL DEFAULT 0` form. A neighboring unique-prefix
option remains `translation_pending` with no target index and no checkpoint
advancement. This is implemented-slice proof only, not full ALTER TABLE coverage,
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
halts progress at that source coordinate. The durable block keeps the process
alive while the reconnect loop retries in-process without consuming the ordinary
transport retry budget. Target-binlog receipt is unavailable; this is semantic
proof only.

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
resolution outside this runtime slice. Until that resolution is proven, the
process remains alive at the unchanged checkpoint and retries the barrier; it
never skips the source event or falls back to raw SQL.

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
