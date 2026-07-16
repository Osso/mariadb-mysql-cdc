# Statement Events

Offline MariaDB `MIXED` fixtures can emit SQL statement events when the server
decides statement replication is safe. Production `stream-binlog` preflights
`binlog_format=ROW` and `binlog_row_image=FULL`; any statement-DML `QueryEvent`
is a contract violation and stops without checkpointing.

## Replay Policy

Production `stream-binlog` rejects statement DML because its source contract
requires `ROW` binlogs with `FULL` row images. The removed `apply-binlog` text
mode is not a supported execution path.

The automatic DDL path is limited to unqualified objects in the configured
application schema. It replays compatible table, index, view, routine, event,
and trigger DDL, including `RENAME TABLE`, `TRUNCATE TABLE`, and `DROP` forms.
Database/schema DDL (`CREATE|ALTER|DROP DATABASE` or `SCHEMA`) is never automatic
even though those prefixes are recognized; it is a manual boundary because the
MySQL target would require global database DDL privileges.

Any explicit qualified identifier (`schema.object`), including backtick and
ANSI_QUOTES double-quoted forms, cross-schema reference, unsafe `DEFINER` or
`SQL SECURITY DEFINER` clause, MariaDB-only syntax, or otherwise disallowed
multi-statement DDL is manual resolution, not automatic replay. Qualification
scanning ignores line and block comment text, so prose punctuation cannot turn
an unqualified DDL statement into a false manual boundary. The manual path
preserves the exact source SQL in the DDL ledger.

The narrow DML allowlist includes:

- `INSERT INTO ...`
- `UPDATE ...`
- `DELETE FROM ...`
- `REPLACE INTO ...`

Statements are normalized by trimming whitespace and a single trailing
semicolon. Replayed statements are sent to the target executor as raw SQL with
no parameters because they came from the source binlog text.

## Schema-changing Query Events

For an unqualified `QueryEvent` whose default database is the configured source
schema, compatible application-object DDL executes automatically. The stream
flushes any grouped DML first, executes the DDL, then saves the event's
`end_log_pos` in a separate target checkpoint transaction and invalidates the
cached target schema. No DDL-ledger row or operator action is required. DDL
execution and checkpoint persistence are not one atomic operation. A target
execution error stops the stream without advancing the checkpoint.

Database/schema DDL, any qualified or cross-schema DDL, unsafe `DEFINER` or
`SQL SECURITY DEFINER` DDL, MariaDB-only forms such as `RETURNING`, `SEQUENCE`,
`SYSTEM VERSIONING`, `VERSIONING`, `DELETE HISTORY`, or `INSERT DELAYED`, and
disallowed multi-statement DDL use manual resolution. The stream flushes earlier
DML, records the exact event and coordinates as `pending`, does not execute it,
and stops before checkpointing past it. An operator must apply and validate the
target change, mark the same ledger row `resolved`, and restart. See the [DDL
Resolution Runbook](ddl-resolution.md).

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
  `LOAD_FILE`, `DEFINER`, or `SQL SECURITY DEFINER`.

Quarantine is a rehearsal artifact, not silent data loss. Every record keeps
the source file, position, default database, raw SQL, and reason so the
specific incompatibility can be patched or marked safe before cutover.
