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

Every other DDL form is manual: tables, other `ALTER TABLE`, views, routines,
events, triggers, `RENAME`, `TRUNCATE`, `DROP` object families other than the
admitted index form, qualified or cross-schema references, comments,
backtick-qualified or ANSI_QUOTES double-quoted identifiers where mode is not
captured, incomplete or ambiguous syntax, definer/security clauses, MariaDB-only
syntax, and multi-object/multi-statement forms.

Before an admitted DDL executes, the stream runs the transformation and captures
immutable evidence from a fenced target pre-state and the translated parsed AST.
The target-side journal records the transformation version and nullable generated
SQL with `prepared`; proven no-ops store NULL, while non-no-ops store the exact
transformed SQL executed. It validates the complete affected state, then records
`applied` and atomically transitions the journal to `checkpointed` with the
predecessor checkpoint update. `prepared` and `blocked` rows form a startup
no-overtake barrier. A crash is never blind replay: only an exact,
unique expected post-state can finalize; pre-state, both/neither, mixed, or
unavailable proof blocks. Target binlog receipt is unavailable, so this is
semantic proof only.

Manual DDL flushes earlier DML, records exact source SQL and coordinates in the
manual ledger, and stops before checkpoint advancement. The ledger and the
automatic journal are separate control-plane objects.

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
- [DDL Resolution Runbook](docs/ddl-resolution.md) for manual boundaries, ledger
  inspection, exact-SQL matching, journal barriers, and restart procedure.
- [One-time journal transformation-evidence upgrade](docs/ddl-replay-journal-transformation-evidence-migration.sql)
  for existing journals, followed by the documented bootstrap rerun.

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
route it through the manual ledger.

See [Catchup Workflow](docs/catchup.md) for bounded repair rules.
