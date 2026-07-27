# Statement Events

Offline MariaDB `MIXED` fixtures can emit statement events. Production
`stream-binlog` preflights `binlog_format=ROW` and `binlog_row_image=FULL`; any
statement DML QueryEvent is a contract violation.

## Replay policy

Statement DML is never replayed in the production stream. The removed
`apply-binlog` text path is not a supported health check.

Automatic DDL admission currently has five narrow slices:

- explicitly named, unqualified, visible, non-unique secondary BTREE `CREATE
  INDEX` or `DROP INDEX` whose key parts and options are completely modeled and
  whose FK dependency is disproven from the fenced target inventory;
- the production-observed unqualified multi-clause `ALTER TABLE` form with
  `ADD COLUMN` under the exact unquoted type grammar
  `VARCHAR(positive canonical decimal length)`, `DATETIME`, or `SMALLINT UNSIGNED`;
  quoted type keywords, quoted `VARCHAR` lengths, and quoted `UNSIGNED` forms are
  rejected, as are `DATETIME` precision and `SMALLINT` display width. The observed
  `DEFAULT NULL`, `NULL`, `COMMENT`, and `AFTER` options, and a named
  composite `ADD KEY` or `ADD UNIQUE KEY`, plus `DROP COLUMN IF EXISTS` with
  ASCII-case-insensitive target matching, one emitted drop per matched target spelling,
  and absent or repeated case-variant no-ops; this path records a
  canonical typed clause AST,
  emits deterministic MySQL 8 SQL, and derives expected post-state from fenced
  target pre-state plus the event AST without requiring the historical source
  head; and
- the production-observed unqualified multi-clause `ALTER TABLE ... RENAME
  COLUMN IF EXISTS ...` form, which is token-parsed and transformed from target
  column pre-state into deterministic MySQL 8 SQL;
- the source-only `CREATE PROCEDURE` statements matching either recorded hash
  for the exact unqualified routine identity
  `apply_release_move_purchase_repair`. Admission precedes generic
  qualified-identifier rejection because the admitted statements contain
  qualified tokens. It requires
  target absence, executes no target SQL, and relies on subsequent source ROW/FULL
  events for data effects in source order;
- the generic exact unqualified, unquoted `DROP PROCEDURE IF EXISTS <identifier>`
  form; and
- the additional exact unqualified, unquoted plain
  `DROP PROCEDURE apply_release_move_purchase_repair` form. Both use target-local
  routine existence evidence: an existing target routine is dropped with
  deterministic quoted MySQL SQL, while an absent routine is a proven no-op.

The index parser rejects comments, ambiguous/incomplete syntax, double-quoted
identifiers when ANSI_QUOTES mode is not captured, qualified names (including
backtick-qualified names), generated names, `IF EXISTS`, unique/fulltext/spatial/
invisible forms, and unmodeled options. Unqualified backtick identifiers are
tokenized; their real-MySQL coverage remains unchecked. The production ALTER
parser does not imply broader `ALTER TABLE` support: types, defaults, clauses,
and index options outside the observed slice remain translation boundaries. The
rename translator removes `IF EXISTS`; absent old columns become a proven no-op,
while old/new coexistence fails closed.

Other table DDL and unsupported `ALTER TABLE` forms, views, other routine DDL,
events, triggers, `RENAME`, `TRUNCATE`, non-admitted `DROP` forms, database/schema DDL,
all other procedure bodies or names, plain drops for other names,
qualified/cross-schema references, quoted or commented forms, other
definer/security clauses, MariaDB-only syntax, and multi-object or multi-statement
forms are translation boundaries in the stream event path. The intended behavior
flushes earlier DML, records exact
SQL/coordinates in `cdc.ddl_replay_journal` as `translation_pending`, and stops
before advancing. The retired manual ledger has no remaining runtime,
configuration, bootstrap, grant, or harness dependency.

Qualifier handling is fail-closed outside the identity-scoped source-only
procedure CREATE form. Tokenization removes comments from syntax but
preserves identifier/dot/identifier detection across inline comments; index
parsing rejects any comment outright. Backticks and ANSI_QUOTES double-quoted
identifiers are not admitted when their mode is unavailable, except for the
private exact-hash source-only procedure admission. Trigger `ON` and index `ON`
references are
qualified checks, not automatic exceptions.

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
supported DDL resolution path in the event handler. The retired manual-ledger
runtime, configuration, bootstrap, grants, and harness paths have been removed.

## Quarantine

Unsupported non-DDL statements are quarantined with source coordinates, raw SQL,
and a reason. Quarantine is not silent data loss and remains a cutover blocker
until reviewed.
