# Statement Events

Offline MariaDB `MIXED` fixtures can emit SQL statement events when the server
decides statement replication is safe. Production `stream-binlog` preflights
`binlog_format=ROW` and `binlog_row_image=FULL`; any statement-DML `QueryEvent`
is a contract violation and stops without checkpointing.

## Replay Policy

Production `stream-binlog` replays the compatible DDL allowlist while rejecting
statement DML because its source contract requires `ROW` binlogs with `FULL` row
images. The removed `apply-binlog` text mode is not a supported execution path. Recognized schema changes
rejected by compatibility policy or whose target schema is ambiguous use manual
DDL resolution instead.

The narrow DML allowlist includes:

- `INSERT INTO ...`
- `UPDATE ...`
- `DELETE FROM ...`
- `REPLACE INTO ...`

Statements are normalized by trimming whitespace and a single trailing
semicolon. Replayed statements are sent to the target executor as raw SQL with
no parameters because they came from the source binlog text.

## Schema-changing Query Events

In `stream-binlog`, compatible source schema-changing `QueryEvent` records are
executed automatically and checkpointed through normal stream handling. This
includes compatible `ALTER TABLE` statements with multiple column additions and
ordinary column-position or comment clauses.

When compatibility policy rejects a recognized schema change, or qualified identifiers make
the target schema ambiguous, the live stream flushes earlier DML, records the
exact event in the target DDL ledger as `pending`, and exits without checkpointing
past it. An operator must apply and validate that target schema change, then
resolve the same ledger record before restart. See the [DDL Resolution
Runbook](ddl-resolution.md).

## Quarantine Policy

The applier quarantines non-DDL statements with exact binlog coordinates when
they are not in the replay allowlist or contain syntax that is risky during a
MariaDB to MySQL migration.

Initial quarantine reasons include:

- Empty statement text.
- Multi-statement text.
- Unsupported non-DDL statement types such as transaction control, session
  changes, procedure calls, privilege changes, or maintenance statements.
- MariaDB-only syntax such as `RETURNING`, sequences, system versioning,
  `DELETE HISTORY`, or `INSERT DELAYED`.
- Unsafe file/privilege patterns such as `LOAD DATA`, `INTO OUTFILE`,
  `LOAD_FILE`, or definer-bound statements.

Quarantine is a rehearsal artifact, not silent data loss. Every record keeps
the source file, position, default database, raw SQL, and reason so the
specific incompatibility can be patched or marked safe before cutover.
