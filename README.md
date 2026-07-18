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
- Keep skipped row changes observable and validate resulting parity before cutover.
- Stop or quarantine unsupported data-changing events with exact coordinates.
- Keep the target out of service until repeated reconciliation proves parity.

## Current status

The native stream applies row events and stores grouped row-event checkpoints in
the target. Automatic DDL admission currently has four narrow slices: an explicitly named,
unqualified, visible, non-unique secondary BTREE `CREATE INDEX` or `DROP INDEX`
when every key part/option is modeled and the operation is proven not to support
or depend on a foreign key; a strict unqualified fixture `CREATE TABLE` grammar
(the harness exercises `accounts`) whose identifiers match
`[A-Za-z_][A-Za-z0-9_]*` after tokenization, with backtick quoting allowed,
comments/double quotes/qualification rejected, one or more `BIGINT` or
`VARCHAR(positive canonical decimal length)` `NOT NULL` columns with at least one
inline `PRIMARY KEY`, zero or more one-column named ordinary `KEY` items, and
`ENGINE=InnoDB` with an optional semicolon; the
production-observed unqualified multi-clause `ALTER TABLE` form with `ADD COLUMN` under the exact unquoted type grammar
`VARCHAR(positive canonical decimal length)`, `DATETIME`, or `SMALLINT UNSIGNED`;
quoted type keywords, quoted `VARCHAR` lengths, and quoted `UNSIGNED` forms are
rejected, as are `DATETIME` precision and `SMALLINT` display width. The observed
`DEFAULT NULL`, `NULL`, `COMMENT`, and `AFTER` options; named composite `ADD KEY` or `ADD UNIQUE KEY` clauses over
ordinary columns; and `DROP COLUMN IF EXISTS`, which matches target column identifiers ASCII-case-insensitively, emits each matched target spelling once, and treats absent or repeated case-variant clauses as proven no-ops. The ALTER path records a typed clause AST and derives expected
post-state by applying that AST to a fenced target pre-state, without requiring a
live source head at the historical event coordinate. The rename slice uses target
column pre-state, emits deterministic MySQL 8 SQL without `IF EXISTS`, treats
absent old columns as a proven no-op, and fails closed when old and new columns
coexist. Broader types/options and full ALTER TABLE remain unsupported.

For admitted `CREATE TABLE`, source schema charset/collation are read only between
exact event-coordinate fences and persisted in immutable evidence; generated SQL
renders them explicitly as `DEFAULT CHARACTER SET ... COLLATE ...`. The target
must be absent before and after capture, and the exact observed post-state must
match the deterministic expected post-state; canonical table evidence sorts
indexes by index name. Unsupported CREATE variants remain `translation_pending`
with no target execution or checkpoint advance.

Every other unsupported DDL form—other `CREATE TABLE` syntax, other `ALTER TABLE`, views, routines,
events, triggers, `RENAME`, `TRUNCATE`, non-admitted `DROP`, qualified or
cross-schema references, comments, ambiguous quoting, incomplete syntax,
definer/security clauses, MariaDB-only syntax, and multi-object/multi-statement
forms—enters the same automatic journal as `translation_pending`. It stores the
exact source identity/coordinates and raw SQL with sentinel
`translator-unavailable`, NULL generated SQL, and empty transformation evidence.
It flushes earlier DML and blocks checkpoint/overtake. The removed manual ledger
is not part of runtime, configuration, bootstrap, grants, or the harness.

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
an FK-aware phased repair planner. Supported constraint conflicts persist
evidence through an independent control-plane connection before the row failure
rolls back the target transaction. Under `ignore-duplicate`, only an equal native
ROW `INSERT` duplicate may be logged and committed without ledger persistence;
divergent inserts and every non-`INSERT` `1062` unique conflict persist evidence,
abort, and leave the target transaction/checkpoint uncommitted. Guarded
observation upserts are idempotent, and the live target checkpoint does not
advance for durable constraint conflicts. The admin-bootstrapped
`cdc.row_conflicts` schema, guards, constraints, definer-safe trigger inventory
procedure, and exact table/procedure grants must validate at startup; runtime never
creates the table. Different source primary keys remain different conflict
identities. `repair-drift` now invokes the planner for child-first deletes,
parent-first inserts, cycle/schema blocking, immutable resumption, bounded PK
windows, a non-mutating full-scope Verify equality phase, and evidence-backed
conflict resolution. The disposable MariaDB 11.4/MySQL 8.0 harness defines 34 executable scenarios.
Earlier TLS harness coverage used a disposable TLS-enabled source, but the live
GlobalComix source MariaDB (`source-mariadb.example` / `192.0.2.10`) is
plaintext-only by accepted operational policy. Current production safety is:
source plaintext only, target DigitalOcean MySQL with configured CA and hostname
verification. The harness proves a valid four-row copy and a completed-run
no-op; it does not prove interrupted parallel-range resume. The
remaining scenarios cover bootstrap/grants, DDL journal crash recovery,
reconnect/GET_LOCK behavior, FK-aware repair/conflict resolution, and a real
`replace-divergent-pk` XID/commit/checkpoint plus replay-evidence scenario. Its
`create-table-crash-restart` scenario passes the differing-default MariaDB/MySQL
fixture through post-DDL/pre-applied
crash recovery, prepared-state restart, exact checkpointing, and idempotent replay;
its `production-alter-table` scenario passes five checkpointed ALTER events; checks column, comment,
non-unique and unique-index metadata, duplicate rejection parity, translated
`DROP COLUMN IF EXISTS`, and its absent-column no-op; then proves
an unsupported unique-prefix option remains `translation_pending` without target
mutation or checkpoint advancement. These are local Docker proofs, not live cutover
proof;
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
  --source-identity app-mariadb-20260710 \
  --binlog-file mysql-bin.000001 --start-position 4 \
  --target-host 127.0.0.1 --target-user cdc_stream \
  --target-password-env TARGET_PASSWORD --target-database app \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem

cargo run -- sync-table --source-host 127.0.0.1 --source-user reader \
  --source-password-env SOURCE_PASSWORD --source-database app \
  --target-host 127.0.0.1 --target-user writer \
  --target-password-env TARGET_PASSWORD --target-database app \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem \
  --table accounts --primary-key id --columns id,email,updated_at \
  --mode apply --run-id accounts-repair-20260710-01
```

```bash
cargo run -- catchup-snapshot \
  --source-host 127.0.0.1 --source-user reader \
  --source-password-env SOURCE_PASSWORD --source-database app \
  --target-host 127.0.0.1 --target-user writer \
  --target-password-env TARGET_PASSWORD --target-database app \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem \
  --progress-file /var/lib/mariadb-mysql-cdc/snapshot-progress.json
```

## TLS policy

The live GlobalComix source MariaDB (`source-mariadb.example` /
`192.0.2.10`) is plaintext-only. Production CDC source connections must not
require or attempt a source CA for that endpoint. Source transport is explicitly
plaintext-only for the current source.

All target-using commands accept `--target-tls-ca-file PATH`; it defaults to
`/etc/mariadb-mysql-cdc/do-ca.pem`. Target DigitalOcean MySQL connections must
use the configured CA and hostname verification. Do not weaken target CA or
hostname verification when changing source transport. See [connection policy](docs/schema-inventory.md#connection-policy).

## Insert conflict policy

`--insert-conflict-policy` is path-specific, not a global “keep CDC running past
duplicates” switch. Values are `error`, `ignore-duplicate`, and the explicit
`replace-divergent-pk` policy:

- Generic target execution treats a MySQL `1062` as success only for statements
  beginning with `INSERT INTO` under `ignore-duplicate`. Other statements and
  errors still fail; `replace-divergent-pk` does not add a generic SQL fallback.
- Native ROW events under `ignore-duplicate` continue only when a duplicate
  `INSERT` target row fetched by source primary key exactly equals the source row.
- Native ROW events under `replace-divergent-pk` read the target row by source
  primary key and replace an unequal row only when the duplicate index is
  `PRIMARY`, using a safe primary-key UPDATE of the source image. The accepted
  risk is overwriting the divergent target row. Successful replacements and
  equal no-ops never create ledger rows; they resolve only an already-recorded
  matching source identity/schema/table/PK record after target commit and
  checkpoint. Secondary-unique, foreign-key, CHECK, and replacement-update
  conflicts persist evidence and abort. If a later conflict rolls back the
  target transaction, the replacement rolls back and the existing ledger record
  remains unresolved.
- With the default `error` policy, native row duplicates fail, roll back the
  target transaction, and leave the checkpoint unchanged.
- `catchup-snapshot` and normal range `sync-table` repairs use explicit
  `INSERT IGNORE` independently of this flag. `sync-table --updated-since`
  uses its own upsert path.

The default policy is `error`.

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
