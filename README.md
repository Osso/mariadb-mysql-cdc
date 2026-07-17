# mariadb-mysql-cdc

Rust migration tooling for moving a MariaDB database to a MySQL-compatible
target with minimal downtime.

## Design constraints

- Consume production `binlog_format=ROW` with `binlog_row_image=FULL`.
- Snapshot table data first, then stream from a recorded binlog position.
- Apply row changes by source primary key; a secondary-unique conflict never
  mutates another target primary key. For a primary-key-changing ROW update,
  assign every writable, non-generated after-image column and predicate on every
  before-image primary-key column.
- Keep skipped conflicts observable and reconcile them before cutover.
- Stop or quarantine unsupported data-changing events with exact coordinates.
- Keep the target out of service until repeated reconciliation proves parity.

## Current status

The native stream applies row events and stores grouped row-event checkpoints in
the target. Automatic DDL admission currently has two narrow slices: an
explicitly named, unqualified, visible, non-unique secondary BTREE `CREATE INDEX`
or `DROP INDEX` when every key part/option is modeled and the operation is proven
not to support or depend on a foreign key; and the production-observed
unqualified multi-clause `ALTER TABLE ... RENAME COLUMN IF EXISTS ...` form.
The rename slice uses target column pre-state, emits deterministic MySQL 8 SQL
without `IF EXISTS`, treats absent old columns as a proven no-op, and fails closed
when old and new columns coexist.

Every other DDL form—tables, other `ALTER TABLE`, views, routines, events,
triggers, `RENAME`, `TRUNCATE`, non-admitted `DROP`, qualified or cross-schema
references, comments, ambiguous quoting, incomplete syntax, definer/security
clauses, MariaDB-only syntax, and multi-object/multi-statement forms—enters the
same automatic journal as `translation_pending`. It stores the exact source
identity/coordinates and raw SQL with sentinel `translator-unavailable`, NULL
generated SQL, and empty transformation evidence. It flushes earlier DML and
blocks checkpoint/overtake. The removed manual ledger is not part of runtime,
configuration, bootstrap, grants, or the harness.

When translator code becomes available, reprocessing the same event captures
immutable pre-state/AST/expected-post-state evidence and promotes that same row
exactly once to `prepared`. The stream executes generated MySQL SQL (or a proven
no-op), validates the complete affected state, transitions `applied`, and
atomically checkpoints. Translation failure and evidence-capture failure use the
same barrier. `translation_pending`, `prepared`, and `blocked` rows stop later
coordinates. A crash is never blind replay: only an exact, unique expected
post-state can finalize; otherwise the row becomes `blocked`. Target binlog
receipt is unavailable, so this is semantic proof only.

No operator-authored target SQL or manual status transition is a supported DDL
resolution path. Fresh bootstrap is the pre-production schema contract; obsolete
development migrations are not maintained as upgrade paths.

The code contains a durable row-conflict ledger wired into the live stream and
an FK-aware phased repair planner. Duplicate and supported constraint conflicts
persist evidence through an independent control-plane connection before the row
failure rolls back the target transaction; guarded observation upserts are
idempotent, and the live target checkpoint does not advance. The admin-bootstrapped
`cdc.row_conflicts` schema, guards, constraints, definer-safe trigger inventory
procedure, and exact table/procedure grants must validate at startup; runtime never
creates the table. Different source primary keys remain different conflict
identities. `repair-drift` now invokes the planner for child-first deletes,
parent-first inserts, cycle/schema blocking, immutable resumption, bounded PK
windows, a non-mutating full-scope Verify equality phase, and evidence-backed
conflict resolution. The disposable MariaDB 11.4/MySQL 8.0 harness defines 30
executable scenarios covering bootstrap/grants, DDL journal crash recovery,
reconnect/GET_LOCK behavior, and FK-aware repair/conflict resolution. Those are
local Docker proofs, not live cutover proof;
recurring conflict scheduling and full cutover proof remain unchecked.

Deployment remains blocked pending real-MySQL/live proof, exact grant/bootstrap
review, bounded repair convergence, and ops rollout gates. Ops proof still needs
fresh immutable image tags, suspended repair/catchup rollout review, replacement
or justification of privileged catchup credentials, unique recurring run IDs,
bounded delete evidence, FK-safe ordering, CA/config-map verification, journal
arguments, and single-writer `GET_LOCK` proof. No ops or deployment action is
part of this worktree. The legacy `probe` text-binlog path is not a supported
health check.

## DDL resolution

- [Authoritative DDL transformation spec](docs/specs/ddl-transformation.md)
- [DDL Resolution Runbook](docs/ddl-resolution.md) for journal barriers,
  translation-pending promotion, evidence inspection, and restart procedure.

## Commands

```bash
cargo run -- plan
cargo run -- stream-binlog --source-host 127.0.0.1 --source-user repl \
  --source-password-env SOURCE_PASSWORD --source-database app \
  --source-tls-ca-file /etc/mariadb-mysql-cdc/source-ca.pem \
  --source-identity app-mariadb-20260710 \
  --binlog-file mysql-bin.000001 --start-position 4 \
  --target-host 127.0.0.1 --target-user cdc_stream \
  --target-password-env TARGET_PASSWORD --target-database app \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem

cargo run -- sync-table --source-host 127.0.0.1 --source-user reader \
  --source-password-env SOURCE_PASSWORD --source-database app \
  --source-tls-ca-file /etc/mariadb-mysql-cdc/source-ca.pem \
  --target-host 127.0.0.1 --target-user writer \
  --target-password-env TARGET_PASSWORD --target-database app \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem \
  --table accounts --primary-key id --columns id,email,updated_at \
  --mode apply --run-id accounts-repair-20260710-01
```

All target-using commands accept `--target-tls-ca-file PATH`; it defaults to
`/etc/mariadb-mysql-cdc/do-ca.pem`. Source/binlog commands accept
`--source-tls-ca-file PATH`; it defaults to `/etc/mariadb-mysql-cdc/source-ca.pem`.
Each file must be readable and contain a valid PEM or DER CA certificate.
Connections fail before the driver runs with an endpoint-specific diagnostic when
that CA is missing, unreadable, or invalid.

`sync-table` requires `--run-id` and stores resumable state in
`cdc.table_sync_runs` by default. A new recurrence needs a new ID; reuse is
allowed only for the exact interrupted run. `repair-drift` creates a fresh
orchestration ID, derives FK-safe phase order, and accepts bounded
`--start-after`/`--end-at` windows. Apply mode requires an explicit
`--max-deletes` allowance.

`--stop-position` is an inclusive event-end boundary: the event whose
`end_log_pos` equals the requested position is applied and durably checkpointed,
then the stream exits. A position inside an event, inside an open row transaction,
or not reached before EOF fails without partial-transaction completion.

The cross-engine inventory query reports `IS_VISIBLE='YES'` for index rows for
MariaDB compatibility. That value is not proof that a MySQL target index is
visible; inspect target-native visibility before admitting affected index DDL, or
leave it in the journal's translation-pending barrier.

See [Catchup Workflow](docs/catchup.md) for bounded repair rules.
