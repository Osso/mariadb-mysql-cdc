# DDL Resolution Runbook

`stream-binlog` never auto-executes a source schema-changing `QueryEvent` on the MySQL target. It flushes earlier DML, records the DDL boundary in a target-side ledger, and stops before checkpointing past it. This is intentional: MariaDB source DDL can be incompatible with the target or require an operational migration plan.

The durable contract is [Manual DDL Resolution](specs/manual-ddl-resolution.md).

## Configuration

The ledger defaults to `cdc.ddl_events`. Set a different qualified table only when the stream configuration and the operator commands below use the same table:

```text
--ddl-ledger-table cdc.ddl_events
```

Before first startup, stop the stream and run
[`ddl-control-plane-bootstrap.sql`](ddl-control-plane-bootstrap.sql) with
resolver/admin credentials. The stream does not create or repair this control
plane. On every startup, before source replication begins, it validates exact
columns, defaults, `ON UPDATE`, status constraint, primary key, both trigger
shapes, and runtime grants; any mismatch fails closed. Its immutable primary key
is:

```text
(source_identity, binlog_file, event_start_position)
```

## Credentials and stream lease

Provision the restricted account from
[`ddl-runtime-grants.sql.example`](ddl-runtime-grants.sql.example) after replacing
the password and reviewing the application schema. Use separate credentials:

- `cdc_stream`: target DML; `SELECT`, `INSERT`, and `UPDATE` on
  `cdc.stream_checkpoint`; and `SELECT`, `INSERT` on `cdc.ddl_events`. It must not
  have `UPDATE`, `DELETE`, `ALTER`, `DROP`, or `TRIGGER` on the ledger, global
  `ALL`, or active role grants. Startup fails closed when those privileges could
  resolve or replace a ledger row. The validated trigger rejects any inserted
  ledger row whose status is not `pending` or whose resolution note is set.
- Resolver/operator credential: reviewed access to apply target schema changes
  and update `cdc.ddl_events.status` and `resolution_note`.

Provision both triggers using the bootstrap SQL or the exact SQL printed by a
missing-trigger startup error. `cdc.ddl_events_pending_insert_guard` enforces
pending-only inserts. `cdc.ddl_events_monotonic_resolution_guard` makes source
identity, coordinates, schema, and raw SQL immutable and permits exactly one
non-empty-note transition from `pending` to `resolved`. Retain both during schema
reviews.

The stream acquires target named lock `cdc-stream:<target database>` without
waiting. Only one stream may target a database; lock failure means another stream
process owns that target and this process exits.

Both the checkpoint row name (`stream-binlog:<source-identity>`) and DDL ledger
identity are scoped to the source incarnation. Before replacing/rebuilding a
source, choose a new identity; never reuse the previous checkpoint row. For an
existing stream migration, stop the old writer, read its final legacy checkpoint,
confirm that binlog file still exists on the source, then clone it once with a
guarded `INSERT ... SELECT` (never UPSERT):

```sql
INSERT INTO cdc.stream_checkpoint (checkpoint_name, checkpoint_json)
SELECT
    'stream-binlog:production-source',
    checkpoint_json
FROM cdc.stream_checkpoint legacy
WHERE legacy.checkpoint_name = 'stream-binlog'
  AND NOT EXISTS (
      SELECT 1
      FROM cdc.stream_checkpoint scoped
      WHERE scoped.checkpoint_name =
          'stream-binlog:production-source'
  );
```

Require exactly one inserted row and verify the legacy/scoped JSON hashes match
before starting the new stream. Retain the legacy row for rollback review.

For a fresh source with no legacy row, capture the exact snapshot/binlog boundary
and insert it explicitly:

```sql
INSERT INTO cdc.stream_checkpoint (checkpoint_name, checkpoint_json)
VALUES (
    'stream-binlog:production-source',
    JSON_OBJECT(
        'source_file', 'mysqld-bin.000001',
        'source_position', 4,
        'gtid', NULL,
        'event_timestamp', 0,
        'last_event', JSON_OBJECT(
            'event_type', 'Bootstrap',
            'description', 'reviewed snapshot/binlog boundary'
        )
    )
);
```

Never invent or default this coordinate. It must match the reviewed snapshot
boundary and its binlog file must still exist on the source.

Pass `--source-identity` as a stable base incarnation ID, for example
`production-source`. Do not include a `#server-id=` suffix. The
stream appends the event server ID when storing the row, producing a value such
as `production-source#server-id=123`. This prevents a binlog
coordinate from one source incarnation/server from resolving another. A row also
stores `source_server_id`, `event_end_position`, `schema_name`, exact `raw_sql`,
`status`, and `resolution_note`.

## Required procedure

### 1. Stop at the DDL boundary

When the stream exits with `manual DDL resolution required`, retain the emitted `source_server_id`, `file`, `start_position`, `end_position`, schema, and SQL. Earlier DML has already been flushed. The DDL has **not** been executed and the checkpoint has **not** advanced past it.

Do not restart repeatedly while the row is pending. It will stop at the same boundary.

### 2. Read the immutable ledger record

Replace the placeholders with values from the stream error. Use the complete endpoint-plus-server-ID `source_identity`, file, and start position; do not identify a DDL by SQL text alone.

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

Copy `raw_sql` directly from this result. The restart path requires byte-for-byte equality with the source event's SQL; editing `raw_sql` to a translated statement will make the stream fail with a ledger SQL mismatch.

### 3. Review, apply, and validate the target schema

Review the exact source DDL and decide the explicit target-side migration. Apply it manually with the target change-control process. The target statement may be a consciously reviewed compatible adaptation, but the ledger's `raw_sql` must remain the unmodified source SQL.

Validate target schema state before resolving. For a table change, inspect the target definition and the specific expected object/column/index, for example:

```sql
SHOW CREATE TABLE globalcomix.accounts\G

SELECT column_name, column_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'globalcomix'
  AND table_name = 'accounts'
ORDER BY ordinal_position;
```

For views, routines, triggers, events, or databases, use the corresponding `SHOW CREATE ...` / `information_schema` check. Record the applied target migration and validation evidence in the resolution note.

**Do not treat a generic target error as success.** Errors such as “already exists”, “doesn't exist”, or “missing object” do not prove the target has the intended schema. They do not resolve the ledger row and do not authorize a checkpoint advance.

### 4. Mark the same row resolved only after validation

After the target change and validation both succeed, update the existing row. Keep the immutable key and `raw_sql` unchanged. Include the applied migration reference and validation evidence in `resolution_note`.

```sql
UPDATE cdc.ddl_events
SET status = 'resolved',
    resolution_note = 'Applied CHG-1234 on target; validated SHOW CREATE TABLE globalcomix.accounts at 2026-07-10T22:00:00Z.'
WHERE source_identity = 'prod-db.example:3306#server-id=123'
  AND binlog_file = 'mysqld-bin.000777'
  AND event_start_position = 123456
  AND status = 'pending'
  AND raw_sql = 'ALTER TABLE accounts ADD COLUMN handle varchar(64)';
```

Confirm exactly one row changed, then re-read it:

```sql
SELECT status, raw_sql, resolution_note, updated_at
FROM cdc.ddl_events
WHERE source_identity = 'prod-db.example:3306#server-id=123'
  AND binlog_file = 'mysqld-bin.000777'
  AND event_start_position = 123456\G
```

**Warning:** marking a row `resolved` before the target DDL has been applied and validated causes source/target schema divergence. On restart, the stream will checkpoint past the source DDL without executing it again.

### 5. Restart and verify progress

Restart the stream with the unchanged checkpoint and identical `--ddl-ledger-table` configuration. It re-reads the ledger record, verifies that its raw SQL exactly matches the source event, advances the checkpoint to `event_end_position`, invalidates its schema cache, and does not replay the DDL.

Confirm the checkpoint has advanced to the recorded `event_end_position`, then monitor for the next event or boundary. This proves only that this DDL boundary was acknowledged; it does not prove whole-database schema or data parity.

## Pending-ledger monitoring

Use this query to find all work blocking CDC, oldest first:

```sql
SELECT
    source_identity,
    source_server_id,
    binlog_file,
    event_start_position,
    event_end_position,
    schema_name,
    raw_sql,
    created_at,
    TIMESTAMPDIFF(MINUTE, created_at, UTC_TIMESTAMP()) AS pending_minutes
FROM cdc.ddl_events
WHERE status = 'pending'
ORDER BY created_at, source_identity, binlog_file, event_start_position;
```

Alert on any returned row. A pending row is an intentional stopped replication boundary, not a retryable transient failure.
