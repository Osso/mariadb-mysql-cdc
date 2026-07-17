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
and multi-object or multi-statement forms are translation boundaries in the
stream event path. The intended behavior flushes earlier DML, records exact
SQL/coordinates in `cdc.ddl_replay_journal` as `translation_pending`, and stops
before advancing. Config/bootstrap/grant/harness cleanup remains open, so this
is not yet a complete operational contract.

Qualifier handling is fail-closed. Tokenization removes comments from syntax
but preserves identifier/dot/identifier detection across inline comments; index
parsing rejects any comment outright. Backticks and ANSI_QUOTES double-quoted
identifiers are not admitted when their mode is unavailable. Trigger `ON` and
index `ON` references are qualified checks, not automatic exceptions.

## Automatic journal

Admitted DDL writes immutable pre-state/AST evidence plus the actual
transformation version and nullable generated SQL to `cdc.ddl_replay_journal` as
`prepared` before execution. A proven no-op stores NULL generated SQL; otherwise
that field is the exact transformed SQL executed. The stream validates the
complete affected target state, transitions to `applied`, and atomically
transitions to `checkpointed` with the exact predecessor checkpoint.
`translation_pending`, `prepared`, and `blocked` rows stop startup from
overtaking the event. Translation and evidence-capture failures use the same
`translation_pending` barrier. Only a unique exact expected post-state can
reconcile a crash; otherwise the row blocks.

No operator-authored target SQL or manual journal status transition is a
supported DDL resolution path in the event handler. Legacy ledger/config/test
symbols and bootstrap/grant/harness dependencies remain open cleanup items.

## Quarantine

Unsupported non-DDL statements are quarantined with source coordinates, raw SQL,
and a reason. Quarantine is not silent data loss and remains a cutover blocker
until reviewed.
