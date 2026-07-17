# DDL Resolution Runbook

`stream-binlog` has two DDL paths:

- **Automatic journal:** strict named, unqualified, visible, non-unique secondary
  BTREE `CREATE INDEX`/`DROP INDEX` with complete parsed metadata and no FK
  dependency, plus the current production-observed unqualified multi-clause
  `ALTER TABLE ... RENAME COLUMN IF EXISTS ...` translator slice. It uses
  `cdc.ddl_replay_journal`.
- **Manual ledger:** every other DDL form. It uses `cdc.ddl_events`.

The stream does not create or repair either control-plane object. Bootstrap must
run with admin/resolver credentials while the stream is stopped. For a new
control plane, run both files in order, then review the resulting grants and
procedure definitions:

```bash
mariadb --defaults-extra-file=/path/admin.cnf < docs/ddl-control-plane-bootstrap.sql
mariadb --defaults-extra-file=/path/admin.cnf < docs/ddl-replay-journal-bootstrap.sql
```

The two files together must match the target fixture contract: exact
`cdc.row_conflicts` `SELECT, INSERT, UPDATE`, checkpoint/journal/ledger scopes,
and application-schema `SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP,
INDEX, REFERENCES, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE,
EXECUTE, EVENT, TRIGGER` grants. Control-plane `EXECUTE` is only on the three
exact trigger-inventory procedures; control-plane/global/admin mutation is
rejected.

For an existing journal created before transformation provenance was persisted,
run the [one-time non-destructive journal upgrade](ddl-replay-journal-transformation-evidence-migration.sql)
while the stream is stopped, then rerun `ddl-replay-journal-bootstrap.sql` to
replace the immutable-evidence trigger and inventory procedure. The upgrade
labels existing rows `legacy-raw-v0` and copies each legacy raw statement into
`generated_sql`; it deletes no rows and does not alter journal identity, state,
or canonical/pre/post-state evidence.

## Startup/bootstrap validation boundary

Bootstrap and startup validate external administrative state once, before source
replication: control-plane columns, keys, checks, guards, trigger-inventory
procedure call results, effective grants, and checkpoint plus the
single-writer `GET_LOCK` prerequisite.
Admin/resolver bootstrap separately reviews `SHOW CREATE PROCEDURE` and direct
trigger rows.
A mismatch is deployment drift and fails fast. The runtime does not recreate or
repair that state.

Binlog DDL is different: it is untrusted source input and is classified per event
against the admission policy before any target operation. Once an event has been
admitted, the CDC-generated SQL is trusted internal program behavior. Event
handling executes the known internal operation directly, performs only the
operation's event-specific pre/post-state or journal checks, and surfaces database
errors. It does not rerun effective-grant policy validation, query `SHOW GRANTS`,
or maintain a second grant/control-plane allowlist.

Manual DDL follows the same boundary. Classification routes the source event to
`cdc.ddl_events`; operator apply/validation is external administrative work and
is not a substitute for startup validation.

## Automatic journal safety

Before source replication, startup validates journal columns, primary key, status
CHECK, immutable insert/update guards, trigger-inventory procedure call results,
effective grants, and the no-overtake barrier. Admin/resolver bootstrap separately
reviews the procedure definition. The expected journal row is:

```text
(source_identity, source_server_id, binlog_file, event_start_position,
 event_end_position, schema_name, raw_sql, transformation_version, generated_sql,
 canonical_ast, pre_state, expected_post_state, status)
```

The state machine is:

```text
prepared -> applied -> checkpointed
prepared -> blocked
```

The stream runs the transformation first, then captures target pre-state and
canonical AST and inserts immutable `transformation_version` plus nullable
`generated_sql` with `prepared` before execution. A proven no-op stores
`generated_sql = NULL`; otherwise the generated MySQL SQL is the exact SQL
executed. It validates the complete affected target state, marks `applied`, then
atomically performs the journal checkpoint transition and predecessor checkpoint
update. `prepared` and `blocked` prevent later source coordinates from overtaking
the event.

A restart never blindly replays `prepared`. It finalizes only an exact unique
expected post-state that differs from pre-state. Pre-state, both/neither, mixed,
or unavailable proof blocks. Target-binlog receipt is not available; this is
semantic evidence only.

The manual ledger and automatic journal may use separate exact
trigger-inventory procedures, but they share one startup/bootstrap grant contract.
The procedure call results are validated as static prerequisites; admin/resolver
reviews the definitions separately. Event handling does not revalidate their grants.

## Prior duplicate-validator failure

The prior implementation called the shared `validate_runtime_grants` path from
`MySqlDdlEventLedger::ensure` while routing a manual DDL event. Startup validation
had already accepted the exact `GRANT EXECUTE ON PROCEDURE
cdc.ddl_replay_journal_trigger_inventory` scope. The second event-path validator
used a narrower per-object policy and rejected that same already-approved exact
inventory grant, so valid manual DDL routing failed despite a passing startup
contract. This was a design error caused by duplicated policy validation, not
missing privilege evidence or a reason to expand the allowlist. The corrected
design keeps static validation at startup/bootstrap only.

## Manual boundary procedure

Every table/view/routine/event/trigger/rename/truncate/non-admitted drop,
other than the supported unqualified multi-clause `ALTER TABLE ... RENAME COLUMN
IF EXISTS ...` slice, plus other `ALTER TABLE`, database/schema DDL, qualified or
cross-schema reference, comments, backtick-qualified or ANSI_QUOTES-ambiguous
identifiers, definer/security clause, MariaDB-only form, incomplete form, or
multi-object/multi-statement form reaches the manual ledger. Unqualified
backtick identifiers are tokenized by the current parser but lack real-MySQL
coverage proof.

The shared inventory query hardcodes `IS_VISIBLE='YES'` for cross-engine
compatibility because MariaDB does not expose a portable visibility column. It
cannot prove that a MySQL target index is visible. If target-native metadata shows
an invisible index in the affected object, keep the DDL manual; do not rely on
automatic admission.

### 1. Stop at the boundary

The stream flushes earlier grouped DML, inserts the exact source identity,
server ID, file, start/end positions, schema, and raw SQL as `pending`, and
stops without advancing past the event. Do not restart repeatedly while the row
is pending.

### 2. Read the immutable row

```sql
SELECT
    source_identity,
    source_server_id,
    binlog_file,
    event_start_position,
    event_end_position,
    schema_name,
    raw_sql,
    status,
    resolution_note,
    created_at,
    updated_at
FROM cdc.ddl_events
WHERE source_identity = 'prod-db.example:3306#server-id=123'
  AND binlog_file = 'mysqld-bin.000777'
  AND event_start_position = 123456\G
```

Copy `raw_sql` exactly. Do not replace it with a translated statement.

### 3. Apply and validate manually

Review the source statement, apply the consciously reviewed target migration, and
validate the complete intended target object with resolver credentials. Generic
errors such as already-exists, missing-object, or object-does-not-exist are not
proof and do not authorize resolution.

Record the applied migration and validation evidence in the resolution note.

### 4. Resolve exactly once

```sql
UPDATE cdc.ddl_events
SET status = 'resolved',
    resolution_note = 'reviewed target migration and validation evidence'
WHERE source_identity = 'prod-db.example:3306#server-id=123'
  AND binlog_file = 'mysqld-bin.000777'
  AND event_start_position = 123456
  AND status = 'pending';
```

Require exactly one row changed. The trigger keeps coordinates, schema, and raw
SQL immutable and permits only one non-empty-note `pending -> resolved` change.

### 5. Restart and verify

Restart with the same source identity and ledger configuration. The stream checks
byte-for-byte raw-SQL equality, advances the checkpoint to `event_end_position`,
invalidates the schema cache, and does not execute the DDL again.

## Runtime grant contract

Required control-plane scopes (separate from the application-schema grant):

- global `USAGE` only;
- `SELECT, INSERT, UPDATE` on `cdc.stream_checkpoint`;
- `SELECT, INSERT, UPDATE` on `cdc.row_conflicts`;
- `SELECT, INSERT` on `cdc.ddl_events`;
- `SELECT, INSERT, UPDATE` on `cdc.ddl_replay_journal`;
- `EXECUTE` only on the exact definer-safe `cdc.row_conflicts_trigger_inventory`, `cdc.ddl_events_trigger_inventory`, and `cdc.ddl_replay_journal_trigger_inventory` procedures.

Reject control-plane/global/admin mutation, `ALL`, `GRANT OPTION`, `PROXY`, roles, broad `cdc.*`,
and row-conflict `DELETE`, `ALTER`, or `DROP` privileges. The startup/bootstrap
validator fails before source streaming when the table, guards, constraints,
procedures, or effective exact grant is missing or widened. Application-schema
privileges used by the stream remain a separate reviewed bootstrap contract.
Runtime calls the exact inventory procedure during startup validation and checks
its returned rows; it does not rerun grant policy validation during event handling.
Admin or resolver credentials must independently run and review SHOW CREATE
PROCEDURE plus the actual trigger rows; runtime does not require SHOW
ROUTINE/global metadata privileges.

## Monitoring

```sql
SELECT
    source_identity,
    source_server_id,
    binlog_file,
    event_start_position,
    event_end_position,
    raw_sql,
    created_at,
    TIMESTAMPDIFF(MINUTE, created_at, UTC_TIMESTAMP()) AS pending_minutes
FROM cdc.ddl_events
WHERE status = 'pending'
ORDER BY created_at, source_identity, binlog_file, event_start_position;

SELECT
    source_identity,
    binlog_file,
    event_start_position,
    status,
    transformation_version,
    generated_sql,
    created_at,
    updated_at
FROM cdc.ddl_replay_journal
WHERE status IN ('prepared', 'blocked')
ORDER BY binlog_file, event_start_position;
```

Any pending manual row or prepared/blocked automatic row is a replication
boundary and a cutover blocker.

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
