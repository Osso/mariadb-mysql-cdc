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

Automatic DDL admission currently covers these narrow slices: explicitly named,
unqualified, visible, non-unique secondary BTREE `CREATE INDEX`/`DROP INDEX`
with complete parsed options and no FK dependency; a strict unqualified fixture
`CREATE TABLE` grammar (the harness exercises `accounts`) whose identifiers match
`[A-Za-z_][A-Za-z0-9_]*` after tokenization, with backtick quoting allowed,
comments/double quotes/qualification rejected, one or more `BIGINT` or
`VARCHAR(positive canonical decimal length)` `NOT NULL` columns with at least one
inline `PRIMARY KEY`, zero or more one-column named ordinary `KEY` items, and
`ENGINE=InnoDB` with an optional semicolon; the exact production
`assistant_reply_reports` `CREATE TABLE` event, admitted only by its exact
raw-event hash after out-of-band target provisioning from the recorded source
definition. Replay requires a stable source inventory with complete table,
index, and foreign-key equality; equality is a proven no-op, while a changed
statement, absent or mismatched target, or moving source fence remains a
barrier without checkpoint advance; and the production-observed unqualified
multi-clause `ALTER TABLE` form with `ADD COLUMN` under the exact unquoted type
grammar `VARCHAR(positive canonical decimal length)`, `DATETIME`, `SMALLINT
UNSIGNED`, or `FLOAT UNSIGNED`, the observed `NULL` or `NOT NULL`, `DEFAULT NULL`
or `DEFAULT 0`, `COMMENT`, and `AFTER` options, and named composite `ADD KEY`,
MariaDB-syntax `ADD INDEX` normalized to the same AST, or `ADD UNIQUE KEY`
clauses. Multiple admitted clauses render in source
order as deterministic MySQL 8 SQL; source `ADD INDEX` emits as target `ADD KEY`.
The slice also admits `DROP COLUMN IF EXISTS` with ASCII-case-insensitive target
matching, one emitted drop per matched target spelling, and absent or repeated
case-variant no-ops; and the production-observed unqualified multi-clause `ALTER TABLE ... RENAME
COLUMN IF EXISTS ...` form. The implemented ALTER path records a canonical typed
clause AST and derives expected post-state by applying that AST to a fenced target
pre-state, so historical replay does not require a live source head at the event
coordinate. The exact production event at
`mysqld-bin.002778:750897987-750898224` has raw SQL SHA-256
`ea9f789b158dca0146715bafe9f2712b5945b9c6626411b382347e60e52eb85f` and is
admitted when this otherwise-supported ALTER has exactly one leading ordinary
MySQL `-- ` line comment. Embedded comments, executable/version comments,
optimizer hints, and all other leading comment forms remain rejected. For the
ALTER `ADD COLUMN` slice, the exact unquoted type grammar is
`VARCHAR(positive canonical decimal length)`, `DATETIME`, `SMALLINT UNSIGNED`, or
`FLOAT UNSIGNED`; quoted type keywords, quoted `VARCHAR` lengths, and quoted
`UNSIGNED` forms are rejected, as are `DATETIME` precision, `SMALLINT` display
width, and `FLOAT` parameters. Unsupported defaults, options, comments, and
clauses remain `translation_pending` with no target DDL or checkpoint advance. For
admitted CREATE TABLE, source charset/collation are read between exact
event-coordinate fences, persisted in evidence, rendered explicitly, and checked
against target absence before and after capture plus the exact observed post-state;
canonical table evidence sorts indexes by index name. The production-observed
source-only `CREATE PROCEDURE` form is admitted only for unqualified routine
identity `apply_release_move_purchase_repair` through a private exact-hash
allowlist. Public documentation intentionally omits raw production procedure
bodies, `DEFINER` hosts, and event coordinates. Admission precedes generic
qualified-identifier rejection because that source statement contains qualified
tokens. Target evidence must
prove the routine absent before and after, no target SQL runs, and later source
ROW/FULL events carry data effects in source order. An existing
`translation_pending` row promotes automatically after identity/header admission.
Other bodies, names, and routine DDL remain `translation_pending` barriers. The
generic exact unqualified, unquoted `DROP PROCEDURE IF EXISTS <identifier>` form
and the additional exact unqualified, unquoted plain
`DROP PROCEDURE apply_release_move_purchase_repair` form are admitted. Target
inventory determines deterministic drop versus proven no-op; qualified, quoted,
commented, and other plain-name forms remain blocked. The exact raw,
unqualified, unquoted, comment-free `DROP TRIGGER IF EXISTS
prevent_deactivating_cloned_archives` form is also admitted, with an optional
trailing semicolon. Stable target trigger inventory emits quoted MySQL `DROP
TRIGGER` when present or records a proven no-op when absent; every other trigger
form remains unsupported.
The rename slice selects executable clauses from target pre-state and emits MySQL 8 SQL without `IF EXISTS`. Every other DDL uses the same
`cdc.ddl_replay_journal` as `translation_pending` with sentinel/no evidence;
translator availability promotes that same row once to `prepared`, after which
generated SQL, postcondition evidence, and checkpointing proceed automatically.
Unsupported or semantically blocked DDL leaves the checkpoint unchanged and
retries the same source coordinate in-process indefinitely after its journal
barrier is durable, without consuming the ordinary transport retry budget,
skipping the event, or executing raw source SQL. The journal state machine is
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
deployment-drift detection. The normal stream path has no multi-writer fence or
fencing-token protocol; the separate lost-binlog recovery path uses exact
transactional CAS checks for its authorized checkpoint/barrier transition.
Binlog DDL remains untrusted input and is classified per event. After admission,
CDC-generated SQL is trusted internal program behavior: event handling executes
known operations directly, keeps only event-specific state/evidence checks, and
surfaces database errors without rerunning grant policy validation or maintaining
duplicate allowlists.

## Lost-binlog recovery

`recover-lost-binlog` is a narrowly authorized availability-first transition
when the live checkpoint names purged MariaDB history. JSON authorization binds
one recovery ID to the exact old checkpoint and exact journal barrier; source
identity and checkpoint name must also match the configured stream. The command
computes the current complete source scope hash and rejects non-InnoDB source
tables. Recovery data repair covers every current source-scope table even when
target-only base tables exist; generic `repair-drift` remains strict about its
source/target inventory contract.

The command acquires the stream lease and captures the current MariaDB binlog
coordinate with ordinary non-locking reads. It does not execute `FLUSH TABLES
WITH READ LOCK`, `UNLOCK TABLES`, or `LOCK TABLES`, does not require `RELOAD`, and
does not keep a cross-table repeatable-read transaction open. Source schema
and row reconciliation use normally committed reads for the attempt's actual
scope. Commits after the captured coordinate remain in the binlog and are
replayed by the stream after recovery advances the checkpoint.

A prepared immutable recovery record links old state, the captured coordinate,
source, the attempt's actual scope, operator, reason, and evidence. After
source-scoped data repair, recovery-only schema convergence drops target-only
base tables child-before-parent with normal foreign-key enforcement; cycles and
source-table references to target-only parents fail closed. The final target
inventory must exactly match the attempt's source inventory before one target
transaction revalidates the exact old state, updates the checkpoint, and commits
only the exact historical barrier supersession. A separately authorized
replacement may atomically mark the exact prepared owner `abandoned` with
server-generated evidence and insert a new `prepared` owner for the same exact
checkpoint, barrier, and source identity. The replacement records its own
current scope; its scope hash need not equal the abandoned owner's. All old
identity, scope, and prepared evidence remain durable. Abandoned history never
suppresses the journal barrier. Only `committed` or `verified` ownership excludes
that exact barrier, and both are terminal. This transition skips purged source
history; it is not replay proof and does not claim production completion until
restart health and subsequent zero-drift verification are recorded.

## Repair model

The code contains a durable conflict schema contract wired into live row-event
handling and an FK-aware phased planner. `cdc.row_conflicts` retains every source
field and uses two lowercase ASCII SHA-256 identities: `conflict_identity`
includes source coordinates and operation, while `source_row_identity` covers
source identity, schema, table, and complete source primary-key JSON for indexed
unresolved-row lookup. The lookup retains every unhashed predicate as a collision
defense. These SHA-256 identities are limited to row conflicts; they do not claim
that FNV-based sync-progress IDs migrated. Supported constraint-conflict evidence is
persisted on an independent connection before the target transaction rolls back,
and the live target checkpoint does not advance. For native ROW `INSERT` changes,
`--insert-conflict-policy ignore-duplicate` skips a `1062` only after the target
row fetched by source primary key exactly equals the source row. A divergent or
otherwise non-equal `ROW INSERT` persists conflict evidence and aborts, rolling
back the target transaction/checkpoint, except for the explicit superseded
historical `globalcomix.users`/`users.name`, `globalcomix.comics`/`comics.slug`,
and approved `globalcomix.releases` FK `ROW INSERT` proofs. Exactly one
candidate is allowed and no ordinary conflict may coexist with it. Each candidate retains its complete historical image; at
XID, `SHOW MASTER STATUS` is read before one source `START TRANSACTION WITH
CONSISTENT SNAPSHOT`, and that pre-snapshot coordinate is only a conservative
lower bound that must be beyond the candidate transaction. The users proof
requires complete source and active-target `FOR UPDATE` hashes for both the
historical primary row and current unique owner. The comics proof requires full
current primary-row equality, while the locked unique owner is accepted by
exact primary-key plus slug identity despite unrelated mutable-field drift, and
only that insert may be treated as a no-op. If typed verification finds that the
source primary still owns the historical identity, it records ordinary unresolved
reconciliation debt, runs no superseded repair SQL, and commits the remaining
transaction with its XID checkpoint; other proof or evidence failures still roll back.
The releases proof is limited to
`releases_ibfk_2` category transaction `mysqld-bin.002709:515816736–515824875`
and `releases_ibfk_3` visibility transaction
`mysqld-bin.002709:531921570–531929925` (candidate event
`531921789`), with the exact child/parent FK identity required. It retains the
complete historical release image, requires one later current source release and
one matching source parent, locks the target release and parent identities, and
installs the complete current release row only when the target release is absent;
an existing target release must hash equal to current source. It preserves the
current parent identity and never updates or deletes that parent. Before
checkpointing, the target transaction must lock an existing same-file predecessor
before the candidate and no later than the XID. Remaining rows still apply, and the observation/resolution evidence
plus XID checkpoint commit atomically; any other proof, predecessor, or commit
failure rolls back, then persists all unresolved observations independently; rollback
or persistence failures are surfaced. When superseded verification rejects a
candidate for any reason other than the typed current-owner result, the structured
error includes the exact parameterized source and
locked-target evidence `SELECT` statements plus the historical primary-key and
unique-identity query parameters; credentials and unrelated row values are never
logged. Every non-`INSERT` `1062` unique conflict also persists evidence
and aborts; all other secondary-unique conflicts remain on that path.
Startup validates the admin-bootstrap schema, guards, constraints, stored
generated `source_row_identity` expression, `(source_row_identity, status)`
index, and exact table/application grants before opening the source stream;
runtime never creates or migrates the table. Existing populated ledgers require
the one-time source-row-identity migration before streaming. `repair-drift` now invokes FK-aware
phases with immutable child runs, cycle/schema blocking, exact chunk verification,
selected PK windows, and a full-scope Verify equality phase before evidence-backed
conflict resolution. In apply mode, `--conflict-reconcile-limit N` also runs a
bounded reconciliation-only pass over unresolved evidence: it reads complete
source and target rows by primary key, resolves only one-row exact equality,
writes no target-table repairs, never reads or advances stream checkpoints, and
is idempotent across repeated passes. The disposable MariaDB 11.4/MySQL 8.0
harness exposes 45 executable scenarios,
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
