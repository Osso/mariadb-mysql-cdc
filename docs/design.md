# Design

## Problem

DigitalOcean Managed MySQL does not accept MariaDB as an online migration source.
MariaDB and MySQL differ in SQL, metadata, and binlog behavior.

## Approach

1. Snapshot existing data in deterministic primary-key chunks.
2. Consume MariaDB ROW/FULL binlog events from the snapshot boundary.
3. Reconcile target drift before cutover; do not serve traffic from an unproven
   target.

## Event handling

Production streaming requires `binlog_format=ROW` and
`binlog_row_image=FULL`. Row events apply by source primary key.

Automatic DDL admission currently covers four narrow slices: explicitly named,
unqualified, visible, non-unique secondary BTREE `CREATE INDEX`/`DROP INDEX`
with complete parsed options and no FK dependency; a strict unqualified fixture
`CREATE TABLE` grammar (the harness exercises `accounts`) whose identifiers match
`[A-Za-z_][A-Za-z0-9_]*` after tokenization, with backtick quoting allowed,
comments/double quotes/qualification rejected, one or more `BIGINT` or
`VARCHAR(positive canonical decimal length)` `NOT NULL` columns with at least one
inline `PRIMARY KEY`, zero or more one-column named ordinary `KEY` items, and
`ENGINE=InnoDB` with an optional semicolon; the production-observed unqualified
multi-clause `ALTER TABLE` form with `ADD COLUMN` under the exact unquoted type
grammar `VARCHAR(positive canonical decimal length)`, `DATETIME`, or `SMALLINT
UNSIGNED`, the observed `DEFAULT NULL`, `NULL`, `COMMENT`, and `AFTER` options,
and named composite `ADD KEY` or
`ADD UNIQUE KEY`, plus `DROP COLUMN IF EXISTS` with ASCII-case-insensitive target
matching, one emitted drop per matched target spelling, and absent or repeated
case-variant no-ops; and the production-observed unqualified multi-clause `ALTER TABLE ... RENAME
COLUMN IF EXISTS ...` form. The implemented ALTER path records a canonical typed
clause AST and derives expected post-state by applying that AST to a fenced target
pre-state, so historical replay does not require a live source head at the event
coordinate. For the ALTER `ADD COLUMN` slice, the exact unquoted type grammar is
`VARCHAR(positive canonical decimal length)`, `DATETIME`, or `SMALLINT UNSIGNED`;
quoted type keywords, quoted `VARCHAR` lengths, and quoted `UNSIGNED` forms are
rejected, as are `DATETIME` precision and `SMALLINT` display width. Those unsupported
variants remain `translation_pending` with no target DDL or checkpoint advance. For
admitted CREATE TABLE, source charset/collation are read between exact
event-coordinate fences, persisted in evidence, rendered explicitly, and checked
against target absence before and after capture plus the exact observed post-state;
canonical table evidence sorts indexes by index name. Unsupported CREATE variants
remain `translation_pending` with no target execution or checkpoint advance. The rename slice selects executable clauses from
target pre-state and emits MySQL 8 SQL without `IF EXISTS`. Every other DDL uses the same
`cdc.ddl_replay_journal` as `translation_pending` with sentinel/no evidence;
translator availability promotes that same row once to `prepared`, after which
generated SQL, postcondition evidence, and checkpointing proceed automatically.
The journal state machine is
`translation_pending -> prepared -> applied -> checkpointed` plus
`prepared -> blocked`; startup barriers prevent overtake. The event-handler behavior is implemented in
this slice, but config/bootstrap/grant/harness cleanup and safe migration rollout
remain open; do not treat manual-ledger removal as deployment-complete.

Unsupported data-changing statements stop or quarantine with exact coordinates.
The old text-binlog probe path is not a production health check.

Static control-plane prerequisites are validated once during admin/bootstrap and
startup, before source replication; see the [DDL Resolution Runbook](ddl-resolution.md#startupbootstrap-validation-boundary).
That validation covers effective grants, control-plane schema, guards, triggers,
procedures, and checkpoint plus single-writer `GET_LOCK` prerequisites as
deployment-drift detection. There is no multi-writer fence, CAS, or fencing-token
protocol.
Binlog DDL remains untrusted input and is classified per event. After admission,
CDC-generated SQL is trusted internal program behavior: event handling executes
known operations directly, keeps only event-specific state/evidence checks, and
surfaces database errors without rerunning grant policy validation or maintaining
duplicate allowlists.

## Repair model

The code contains a durable conflict schema contract wired into live row-event
handling and an FK-aware phased planner. `cdc.row_conflicts` uses a lowercase
ASCII SHA-256 `conflict_identity` primary key over the canonical full source
identity tuple while retaining every source field for collision checks. This
SHA-256 statement is limited to conflict identities; it does not claim that
FNV-based sync-progress IDs migrated. Supported constraint-conflict evidence is
persisted on an independent connection before the target transaction rolls back,
and the live target checkpoint does not advance. For native ROW `INSERT` changes,
`--insert-conflict-policy ignore-duplicate` skips a `1062` only after the target
row fetched by source primary key exactly equals the source row. A divergent or
otherwise non-equal `ROW INSERT` persists conflict evidence and aborts, rolling
back the target transaction/checkpoint, except for one explicit superseded
historical `globalcomix.users` ROW `INSERT` on exact `users.name`. That candidate
retains its complete historical image; at XID, one source consistent snapshot
must be beyond the candidate transaction, both complete source rows and both
active-target `FOR UPDATE` rows must satisfy exact full-row hash predicates, and
only that insert may be treated as a no-op. Remaining rows still apply, and the
observation/resolution evidence plus XID checkpoint commit atomically; any proof
or commit failure rolls back. Every non-`INSERT` `1062` unique conflict also
persists evidence and aborts; all other secondary-unique conflicts remain on
that path.
Startup validates the admin-bootstrap schema, guards, constraints, and exact
table/application grants before opening the source stream; runtime never creates
the table. `repair-drift` now invokes FK-aware
phases with immutable child runs, cycle/schema blocking, explicit delete ceilings,
selected PK windows, and a full-scope Verify equality phase before evidence-backed
conflict resolution. The disposable MariaDB 11.4/MySQL 8.0 harness exposes 44 executable scenarios,
including catchup, repair, conflict, DDL, and reconnect boundaries, plus a real
`replace-divergent-pk` XID/commit/checkpoint and replay-evidence scenario. The live
GlobalComix source MariaDB is plaintext-only by accepted operational policy;
target MySQL remains CA- and hostname-verified. The catchup scenario proves a
valid four-row copy and a completed-run no-op. It does not prove interrupted
parallel-range resume. Its
`create-table-crash-restart` scenario passes the differing-default fixture
through post-DDL/pre-applied crash recovery, prepared-state restart, exact
checkpointing, and idempotent replay; its `production-alter-table` scenario passes
five checkpointed ALTER events,
checks column/comment/non-unique and unique-index metadata, duplicate rejection
parity, translated `DROP COLUMN IF EXISTS`, and its absent-column no-op, and proves an unsupported unique-prefix option remains pending
without target mutation or checkpoint advancement. These are local proofs for implemented
boundaries. Broader ALTER coverage, the full compatibility matrix, live recurring
scheduling, deployment, and cutover gates remain unchecked.

## Safety and validation

- Checkpoint grouped target DML transactions.
- Validate journal/ledger schema, guards, routines, exact grants, and the
  single-writer `GET_LOCK` state once before source replication; do not repeat
  this static policy per event.
- Use stable primary-key windows for count/content checks.
- Treat unresolved conflicts, quarantine, journal barriers, schema drift, and
  CA/grant gaps as blockers. `translation_pending` is cleared only by translator
  code and automatic promotion in the event path; config/bootstrap/grant/harness
  dependencies and migration safety remain open.
- Keep the target out of service through repeated validation and cutover review.
