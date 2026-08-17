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

## Build prerequisites

Parallel target submission uses MariaDB Connector/C through `mysqlclient-sys` so
query send and result completion remain separate operations. Local builds require
`pkg-config` plus MariaDB client development files. The Docker builder installs
`libmariadb-dev`; the selected runtime `BASE_IMAGE` must provide
`libmariadb.so.3`.

## Deployment

`deploy.sh` requires `IMAGE_REPO` and `BASE_IMAGE`:

```sh
IMAGE_REPO=registry.example/mariadb-mysql-cdc \
BASE_IMAGE=registry.example/mariadb:tag \
./deploy.sh [TAG]
```

`OPS_REPO` is optional and defaults to the sibling `../ops` checkout. The script
passes `BASE_IMAGE` to Docker as a build argument for the runtime image.

## Current status

The native stream applies row events and stores grouped row-event checkpoints in
the target. Automatic DDL admission currently covers these narrow slices: an explicitly named,
unqualified, visible, non-unique secondary BTREE `CREATE INDEX` or `DROP INDEX`
when every key part/option is modeled and the operation is proven not to support
or depend on a foreign key; a strict unqualified fixture `CREATE TABLE` grammar
(the harness exercises `accounts`) whose identifiers match
`[A-Za-z_][A-Za-z0-9_]*` after tokenization, with backtick quoting allowed,
comments/double quotes/qualification rejected, one or more `BIGINT` or
`VARCHAR(positive canonical decimal length)` `NOT NULL` columns with at least one
inline `PRIMARY KEY`, zero or more one-column named ordinary `KEY` items, and
`ENGINE=InnoDB` with an optional semicolon; production-observed source-only `CREATE PROCEDURE` form for the exact
unqualified routine identity `apply_release_move_purchase_repair`, admitted only
by a private exact-hash allowlist. Public documentation intentionally omits raw
production procedure bodies, `DEFINER` hosts, and event coordinates. Admission
occurs before the generic qualified-identifier check because that source
statement contains qualified
tokens. The target routine must be absent before and after evidence capture, no
target SQL runs, and later ROW/FULL events carry data effects in source order;
the
production-observed unqualified multi-clause `ALTER TABLE` form with `ADD COLUMN` under the exact unquoted type grammar
`VARCHAR(positive canonical decimal length)`, `DATETIME`, or `SMALLINT UNSIGNED`;
quoted type keywords, quoted `VARCHAR` lengths, and quoted `UNSIGNED` forms are
rejected, as are `DATETIME` precision and `SMALLINT` display width. The observed
`DEFAULT NULL`, `NULL`, `COMMENT`, and `AFTER` options; named composite `ADD KEY` or `ADD UNIQUE KEY` clauses over
ordinary columns; and `DROP COLUMN IF EXISTS`, which matches target column identifiers ASCII-case-insensitively, emits each matched target spelling once, and treats absent or repeated case-variant clauses as proven no-ops. Two routine-drop forms are admitted: the generic exact unqualified, unquoted
`DROP PROCEDURE IF EXISTS <identifier>` form, and the exact unqualified,
unquoted plain `DROP PROCEDURE apply_release_move_purchase_repair` form.
Target-local routine inventory determines the result: an existing routine emits
deterministic MySQL `DROP PROCEDURE` using the target spelling backtick-quoted;
an absent routine emits no target SQL as a proven no-op. Qualified, quoted,
commented, and other plain-name procedure drops remain `translation_pending`
barriers. The exact raw, unqualified, unquoted, comment-free
`DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives` form, with an
optional trailing semicolon, is admitted. Stable target trigger inventory emits
quoted MySQL `DROP TRIGGER` when present or records a proven no-op when absent;
all other trigger variants remain `translation_pending`. The ALTER path records a typed clause AST and derives expected
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
indexes by index name. The admitted source-only procedure form is a target no-op: target evidence must
prove `apply_release_move_purchase_repair` absent before and after capture,
target SQL remains absent, and any data effects arrive through subsequent source
ROW/FULL events in source order. An existing `translation_pending` row promotes
automatically after identity/header admission. Unsupported CREATE variants
remain `translation_pending` with no target execution or checkpoint advance.

Every other unsupported DDL form—other `CREATE TABLE` syntax, other `ALTER TABLE`, views, other routine DDL,
events, unsupported trigger DDL, `RENAME`, `TRUNCATE`, non-admitted `DROP`, qualified or
cross-schema references, comments, ambiguous quoting, incomplete syntax,
other procedure bodies or names, other plain procedure drops, other definer/security
clauses, MariaDB-only syntax, and multi-object/multi-statement forms—enters the same automatic journal
as `translation_pending`. It stores the exact source identity/coordinates and raw
SQL with sentinel
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
resolution path. Fresh bootstrap remains the pre-production schema contract.
Existing populated `cdc.row_conflicts` tables use
`docs/row-conflicts-source-row-identity-migration.sql` once while offline repair
writers are stopped; runtime never performs this migration. Obsolete
development migrations are not maintained as upgrade paths.

Native ROW streaming is source-authoritative and target-disposable. It emits plain
INSERT statements and treats MySQL `1062` from INSERT as idempotent success,
without target inspection, equality checks, replacement, conflict evidence, or
repair. Every other row error rolls back the complete source transaction and
blocks checkpoint advancement. `--target-parallel-transactions N` preserves the
same rule by sending and draining each statement individually, leasing one target
connection per complete source transaction, and committing checkpoints in source
order.

Conflict data and repair remain offline. `cdc.row_conflicts`, `repair-drift`,
targeted conflict resolution, FK-aware table-sync ordering, bounded primary-key
windows, and exact verification are retained, but the live stream neither reads
nor writes the conflict ledger and does not validate its schema, procedures, or
grants. Retired live supersession, target-replacement, and automatic parent-repair
paths are absent from runtime and harness code.

The disposable MariaDB/MySQL harness continues to cover catchup, offline repair,
DDL journal recovery, reconnect/GET_LOCK behavior, and parallel target
transactions. These are local proofs, not live cutover proof.

Deployment remains blocked pending real-MySQL/live proof, exact grant/bootstrap
review, bounded repair convergence, and ops rollout gates. Ops proof still needs
fresh immutable image tags, suspended repair/catchup rollout review, replacement
or justification of privileged catchup credentials, unique recurring run IDs,
exact chunk verification, FK-safe ordering, CA/config-map verification, journal
arguments, and single-writer `GET_LOCK` proof. No ops or deployment action is
part of this worktree. The legacy `probe` text-binlog path is not a supported
health check.

## DDL resolution

- [Authoritative DDL transformation spec](docs/specs/ddl-transformation.md)
- [DDL Resolution Runbook](docs/ddl-resolution.md) for journal barriers,
  translation-pending promotion, evidence inspection, and restart procedure.
- [Schema synchronization spec](docs/specs/sync-schema.md) for selected-table
  full convergence through the shared streamed-DDL translator.

## Schema synchronization

The implemented `sync-schema` command applies by default and converges only explicitly
selected tables to the source-authoritative MySQL 8-compatible schema. It runs one
table at a time, permits destructive changes within selected tables, and never
drops unselected target tables. Every mapping must use the same DDL translator as
streamed DDL replay; there is no direct-source-DDL fallback or second compatibility
mapping.

Before a potentially lossy column change, it checks actual target data. Values that
would truncate, coerce, or fail block that table and produce representative primary
keys; independent tables continue, while dependent operations are skipped. The
command re-inventories every selected table and emits structured JSON for every
statement, preflight, skip, error, and verification result. It exits nonzero if
anything remains divergent. It has no persistent schema journal or rollback claim
for implicitly committed MySQL DDL.

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

cargo run -- table-catalog \
  --source-host 127.0.0.1 --source-user reader \
  --source-password-env SOURCE_PASSWORD --source-database app \
  --target-host 127.0.0.1 --target-user writer \
  --target-password-env TARGET_PASSWORD --target-database app \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem \
  --syncable-output syncable-tables.json \
  --non-syncable-output full-dump-tables.json

cargo run -- sync-catalog \
  --source-host 127.0.0.1 --source-user reader \
  --source-password-env SOURCE_PASSWORD --source-database app \
  --target-host 127.0.0.1 --target-user writer \
  --target-password-env TARGET_PASSWORD --target-database app \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem \
  --catalog syncable-tables.json --run-id-prefix catalog-20260722
```

`stream-binlog --target-parallel-transactions N` enables bounded target
transaction submission when `N > 1`; the default `1` preserves serial execution.
The parallel path sends and drains body statements individually, ignores delayed
`1062` only for INSERT statements, and fails closed before checkpoint advancement
on every other delayed target error.

Run the disposable source-authoritative proofs with:

```sh
python3 scripts/cdc-integration-harness.py --scenario insert-duplicate-idempotent
python3 scripts/cdc-integration-harness.py --scenario parallel-target-transactions
```

The serial scenario preloads a divergent target row, removes the conflict ledger
and inventory procedure, and proves the duplicate leaves that row untouched while
a later same-transaction row and exact checkpoint advance. The parallel scenario
adds ordered-commit barriers, verifies every target session uses `SSL/TLS`, and
proves both transactions converge only after release. Offline repair scenarios
retain the separate ledger, comparison, and FK-repair identities.

### Table catalog JSON and execution contract

`table-catalog` writes two pretty JSON objects, each with a top-level `tables`
array. Syncable entries have exactly `name`, `primary_key`, `columns`,
`estimated_source_rows`, and `parent_dependencies`; non-syncable entries have
exactly `name`, `estimated_source_rows`, and `reasons`:

```json
{
  "tables": [
    {
      "name": "orders",
      "primary_key": ["id"],
      "columns": ["id", "updated_at"],
      "estimated_source_rows": 123,
      "parent_dependencies": ["users"]
    }
  ]
}
```

```json
{
  "tables": [
    {
      "name": "audit_log",
      "estimated_source_rows": 456,
      "reasons": ["missing_primary_key"]
    }
  ]
}
```

Reason codes are `missing_primary_key`, `missing_target_table`,
`incompatible_schema`, `unsupported_generated_columns`,
`cross_schema_dependency`, and `dependency_on_non_syncable`. A source table
whose source or target FK references another schema receives
`cross_schema_dependency`; a same-named local table does not satisfy that
dependency. Entries
may carry multiple reasons: dependency
propagation preserves existing exclusion reasons and adds
`dependency_on_non_syncable` transitively to every affected descendant. Catalog
arrays are ordered by estimated source rows, then table name; primary-key and
writable-column arrays retain inventory order, parent-dependency arrays are
unique and lexicographically ordered from the union of applicable source and
target FKs, and reason arrays use enum declaration order. Target-only local FKs
gate scheduling. FK locality is evaluated against the schema owning each
inventory, so a target FK referencing the source schema remains cross-schema.
Compatibility requires matching table default character sets (derived from table
collations) and exact per-column `CHARACTER_SET_NAME`/`COLLATION_NAME` values for
corresponding writable columns; a mismatch is `incompatible_schema`.
`table-catalog` rejects output paths that resolve to the same filesystem
destination, including lexical aliases, existing symlinks,
intermediate-symlink-plus-`..` aliases, and hardlinks, and fails closed on
symlink cycles. Resolution canonicalizes the longest existing physical ancestor
before normalizing any nonexistent suffix. After catalog generation, it opens
both outputs without truncation, compares the opened file identities, and only
then truncates and writes through those handles. Path changes cannot redirect
the second write over the first. If both destinations were nonexistent, a failed
final identity check may leave an empty newly created file, but never overwrites
existing content.

`sync-catalog` reads only the syncable catalog and starts apply mode immediately;
there is no dry-run/plan mode and `table-catalog` does not launch it. The command
waits until all entries complete or a failure is returned. Each table uses a
fixed-length `catalog-v2-<SHA-256 hex>` run ID. The digest input is the
injective length-framed byte tuple `(run-id prefix, target database, table)`, so
prefix, database, and table changes produce distinct deterministic IDs while
long valid names still fit the 128-byte progress column. The prefix is required
and non-empty. Interrupted exact IDs resume; a matching `status='complete'` row
is terminal only when its immutable run specification exactly matches the current
catalog child; a different stored specification fails closed instead of being
treated as terminal. Direct `sync-table` and
`sync-catalog` workers share one admission lock and four slot reservations keyed
by the lower-cased target host plus port, so databases on the same host and port
share capacity. Each worker also holds a database/table-specific reservation,
preventing same-table overlap. Database and table components are length-framed
and hexadecimal-encoded, so delimiters inside identifiers cannot alias another
reservation. These admission and slot locks are scoped to the
target server host and port. Legacy run-lock accounting and same-table detection
inspect only the configured progress table. Within that table, identity is
derived from the immutable run specification's `scope.target_database` and
`table.name`, not from `table_name` alone, so same-named tables in different target
databases remain distinct and cannot satisfy each other's dependencies. A held
legacy run-ID advisory lock for the requested same database/table excludes that
table even without a table reservation. Rows in `running`, `complete`, or
`error` with a held legacy run-ID advisory lock but no table reservation count
toward the same four-worker limit, including rows for tables outside the supplied
catalog. Unlocked stale `running` or `error` rows are ignored before parsing their
immutable specifications. Lock-active rows fail closed when malformed. An
expected completed child is always parsed and must exactly match its immutable
specification before it is treated as terminal. Reservation sessions set MySQL `wait_timeout` to
86,400 seconds so an idle lock connection can span long table runs; this does not
provide recovery from network disconnects. Children start only after all listed FK parents complete. Missing
dependencies are rejected before workers start; owned failures return after owned
work settles, and dependency cycles fail closed without waiting for unrelated
external syncs. Catalog children reconcile every target-only row planned by
catalog comparison in dependency-safe chunks, verify each deletion, and persist
progress only after verification. The same unconditional chunk reconciliation
applies to direct `sync-table` and `repair-drift`; the non-syncable catalog is
classification/operator input only; full-dump execution is out of scope.

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

The `table-catalog` and `sync-catalog` commands require an explicit, non-empty
`--target-tls-ca-file PATH`; their command contract defines no default path.
Target DigitalOcean MySQL connections must use the configured CA. DNS/hostname
endpoints must also verify certificate identity; IP endpoints retain CA/chain
validation but skip DNS name matching. Do not weaken target CA or hostname
verification when changing source transport. See [connection policy](docs/schema-inventory.md#connection-policy).

## Insert conflict policy

`--insert-conflict-policy` applies to statement replay and offline
snapshot/table-sync paths. Values are `error`, `ignore-duplicate`, and
`replace-divergent-pk`:

- Generic target execution treats MySQL `1062` as success only for statements
  beginning with `INSERT INTO` under `ignore-duplicate`.
- Native ROW streaming does not use this policy. Its fixed rule accepts INSERT
  `1062` and fails every other row error.
- Snapshot modes may explicitly select upsert or duplicate-ignore behavior.
- Table-sync missing-row and displaced-owner repair retains its bounded,
  verified `replace-divergent-pk` behavior.

`sync-table --mode missing-primary-keys` is apply-only: it compares source and
target rows by primary key, inserts source rows whose primary keys are absent,
and never deletes dependent rows. With `replace-divergent-pk`, an exact one-hop
displacement is repaired transactionally: the displaced target owner is restored
from the same source chunk, the missing owner is inserted, affected child rows
are verified unchanged, and run progress commits on the same target connection.
Ambiguous chains, absent source owners, verification failures, and constraint
failures roll back parents and progress. Other conflicts remain errors. The
sync-table source, target, and progress connections use a 10-second TCP connect
timeout and 30-second read/write operation timeouts. All MySQL connections
share TCP liveness bounds; live CDC/DDL connections do not use the sync
operation timeouts. Transient connection failures retry up to five attempts
total (the initial attempt plus four retries), with each retry resuming from
durable `cdc.table_sync_runs` progress; non-transient errors and exhausted
retries fail.

The default policy is `error`.

`sync-table` requires `--run-id` and stores resumable state in
`cdc.table_sync_runs` by default. A new recurrence needs a new ID; direct
`sync-table` reuse is allowed only for the exact interrupted run. In apply mode,
`repair-drift` may reclaim exactly one failed `missing-primary-keys` child whose
full immutable specification matches the current insert phase. The claim uses a
per-transaction `REPEATABLE READ` transaction with `FOR UPDATE` candidate
locking; ambiguity fails closed. `repair-drift` otherwise creates a fresh
orchestration ID, derives FK-safe phase order, and accepts bounded
`--start-after`/`--end-at` windows. Each completed chunk is verified before
its cursor and counters are persisted.

`--stop-position` is an inclusive event-end boundary: the event whose
`end_log_pos` equals the requested position is applied and durably checkpointed,
then the stream exits. A position inside an event, inside an open row transaction,
or not reached before EOF fails without partial-transaction completion.

The cross-engine inventory query reports `IS_VISIBLE='YES'` for index rows for
MariaDB compatibility. That value is not proof that a MySQL target index is
visible; inspect target-native visibility before admitting affected index DDL, or
leave it in the journal's translation-pending barrier.

See [Catchup Workflow](docs/catchup.md) for bounded repair rules.
