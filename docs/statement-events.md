# Statement Events

MariaDB `MIXED` binlogs can emit SQL statement events when the server decides
statement replication is safe. This tool treats that as an explicit policy
decision instead of blindly replaying every `QueryEvent`.

## Replay Policy

The first pass only replays narrow, known-compatible DML:

- `INSERT INTO ...`
- `UPDATE ...`
- `DELETE FROM ...`
- `REPLACE INTO ...`

Statements are normalized by trimming whitespace and a single trailing
semicolon. Replayed statements are sent to the target executor as raw SQL with
no parameters because they came from the source binlog text.

## Quarantine Policy

The applier quarantines statements with exact binlog coordinates when they are
not in the replay allowlist or when they contain syntax that is risky during a
MariaDB to MySQL migration.

Initial quarantine reasons:

- Empty statement text.
- Multi-statement text.
- Unsupported statement type such as DDL, transaction control, session changes,
  procedure calls, privilege changes, or maintenance statements.
- MariaDB-only syntax such as `RETURNING`, sequences, system versioning,
  `DELETE HISTORY`, or `INSERT DELAYED`.
- Unsafe file/privilege patterns such as `LOAD DATA`, `INTO OUTFILE`,
  `LOAD_FILE`, or definer-bound statements.

Quarantine is a rehearsal artifact, not silent data loss. Every record keeps
the source file, position, default database, raw SQL, and reason so the
specific incompatibility can be patched or marked safe before cutover.
