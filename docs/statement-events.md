# Statement Events

Offline MariaDB `MIXED` fixtures can emit statement events. Production
`stream-binlog` preflights `binlog_format=ROW` and `binlog_row_image=FULL`; any
statement DML QueryEvent is a contract violation.

## Replay policy

Statement DML is never replayed in the production stream. The removed
`apply-binlog` text path is not a supported health check.

Automatic DDL admission currently has two slices:

- explicitly named, unqualified, visible, non-unique secondary BTREE `CREATE
  INDEX` or `DROP INDEX` whose key parts and options are completely modeled and
  whose FK dependency is disproven from the fenced target inventory;
- the production-observed unqualified multi-clause `ALTER TABLE ... RENAME
  COLUMN IF EXISTS ...` form, which is token-parsed and transformed from target
  column pre-state into deterministic MySQL 8 SQL.

The index parser rejects comments, ambiguous/incomplete syntax, double-quoted
identifiers when ANSI_QUOTES mode is not captured, qualified names (including
backtick-qualified names), generated names, `IF EXISTS`, unique/fulltext/spatial/
invisible forms, and unmodeled options. Unqualified backtick identifiers are
tokenized; their real-MySQL coverage remains unchecked. The rename translator
removes `IF EXISTS`; absent old columns become a proven no-op, while old/new
coexistence fails closed.

Other tables, `ALTER TABLE`, views, routines, events, triggers, `RENAME`,
`TRUNCATE`, non-admitted `DROP` forms, database/schema DDL,
qualified/cross-schema references, definer/security clauses, MariaDB-only syntax,
and multi-object or multi-statement forms are manual boundaries. The stream flushes earlier DML,
records exact SQL/coordinates in `cdc.ddl_events`, and stops before advancing.

Qualifier handling is fail-closed. Tokenization removes comments from syntax
but preserves identifier/dot/identifier detection across inline comments; index
parsing rejects any comment outright. Backticks and ANSI_QUOTES double-quoted
identifiers are not admitted when their mode is unavailable. Trigger `ON` and
index `ON` references are qualified checks, not automatic exceptions.

## Automatic journal

Admitted DDL writes immutable pre-state/AST evidence to
`cdc.ddl_replay_journal` as `prepared` before execution. The stream validates the
complete affected target state, transitions to `applied`, and atomically
transitions to `checkpointed` with the exact predecessor checkpoint. `prepared`
and `blocked` rows stop startup from overtaking the event. Only a unique exact
expected post-state can reconcile a crash; otherwise the row blocks.

## Quarantine

Unsupported non-DDL statements are quarantined with source coordinates, raw SQL,
and a reason. Quarantine is not silent data loss and remains a cutover blocker
until reviewed.
