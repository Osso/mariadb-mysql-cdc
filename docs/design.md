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

Automatic DDL admission currently covers two slices: explicitly named,
unqualified, visible, non-unique secondary BTREE `CREATE INDEX`/`DROP INDEX`
with complete parsed options and no FK dependency, plus the production-observed
unqualified multi-clause `ALTER TABLE ... RENAME COLUMN IF EXISTS ...` form.
The rename slice selects executable clauses from target pre-state and emits
MySQL 8 SQL without `IF EXISTS`. Every other DDL uses the same
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
FNV-based sync-progress IDs migrated. Conflict evidence is persisted on an
independent connection before the target transaction rolls back, and the live
target checkpoint does not advance. Startup validates the admin-bootstrap schema,
guards, constraints, and exact table/application grants before opening the source
stream; runtime never creates the table. `repair-drift` now invokes FK-aware
phases with immutable child runs, cycle/schema blocking, explicit delete ceilings,
selected PK windows, and a full-scope Verify equality phase before evidence-backed
conflict resolution. The disposable MariaDB 11.4/MySQL 8.0 harness exposes 30
executable scenarios; its local proofs pass for the implemented boundaries. Live
recurring scheduling, deployment, and cutover gates remain unchecked.

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
