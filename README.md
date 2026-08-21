# mariadb-mysql-cdc

Rust migration tooling for moving a MariaDB database to a MySQL-compatible
target with minimal downtime.

## Design constraints

- Consume production `binlog_format=ROW` with `binlog_row_image=FULL`.
- Use staged `sync` for source-authoritative convergence while the live ROW stream handles ongoing changes.
- Apply row changes by source primary key; a secondary-unique conflict never
  mutates another target primary key. For a primary-key-changing ROW update,
  assign every writable, non-generated after-image column and predicate on every
  before-image primary-key column.
- Keep skipped row changes observable and validate resulting parity before cutover.
- Stop or quarantine unsupported data-changing events with exact coordinates.
- Keep the target out of service until repeated reconciliation proves parity.

## Build prerequisites

Live target execution uses Rust's `mysql` client on one initialized target
connection. The removed parallel submission path no longer requires MariaDB
Connector/C, `mysqlclient-sys`, `pkg-config`, or `libmariadb` packages. The
Ubuntu 24.04 runtime remains pinned to a verified OCI index digest, upgrades
installed packages during the build, and installs only CA certificates and the
libraries required by the built binary.

## Runtime image verification

Build and behaviorally verify a local candidate without publishing it:

```sh
docker build --tag mariadb-mysql-cdc:runtime-test .
python3 tests/verify_runtime_image.py mariadb-mysql-cdc:runtime-test
```

The verifier checks the built image rather than Dockerfile text: Ubuntu 24.04,
fixed numeric UID/GID `65532:65532`, direct entrypoint execution, CA certificates,
readability of a read-only mounted CA file under the runtime identity, required
packages and linked libraries, and absence of `gosu`.

## Deployment

`deploy.sh` requires only `IMAGE_REPO`:

```sh
IMAGE_REPO=registry.example/mariadb-mysql-cdc ./deploy.sh [TAG]
```

The runtime base is fixed by digest in the Dockerfile; there is no `BASE_IMAGE`
build contract. `OPS_REPO` is optional and defaults to the sibling `../ops`
checkout. `DEPOT_PROJECT_ID` optionally selects the Depot project and defaults
to `jnnl97r4s7`; `PUSH_OPS=0` keeps the verified ops commit local instead of
pushing it.

Unless `SKIP_VERIFIED_CHECKS=1`, `deploy.sh` runs formatting, the repository
`./run-tests.sh` path, and Clippy before building. The repository test path runs
both Rust tests and `tests/test_deploy_script.py`.

Depot publishes the candidate tag before deployment admission. `deploy.sh` then
resolves the tag's immutable manifest digest, pulls and behaviorally verifies the
exact `tag@digest`, and scans that digest through the Docker socket with pinned
Trivy 0.73.0. The scan covers vulnerability findings at HIGH and CRITICAL
severity, ignores unfixed findings, skips Trivy's version check, and fails closed
through its exit code. Only after image verification and the vulnerability gate
pass does the script write the exact `repo:tag@sha256:...` reference to the live
stream manifest, then commit or push it. A failed gate may leave the candidate in
the registry but leaves ops reconciliation unchanged. Unified sync Jobs remain
reviewed and managed separately; `deploy.sh` does not create or update them.

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
`docs/row-conflicts-source-row-identity-migration.sql` once while out-of-band repair
writers are stopped; runtime never performs this migration. Before deploying the
new live-stream image, accounts provisioned under the older runtime-grant contract
must also run
`docs/live-stream-runtime-grants-migration-20260818.sql` with target admin
credentials. That one-time migration revokes obsolete live-stream access to the
historical conflict ledger and legacy table-sync progress without deleting those
objects or changing resolver access. Obsolete development migrations are not
maintained as upgrade paths.

Native ROW streaming is source-authoritative and target-disposable. It emits plain
INSERT statements and treats MySQL `1062` from INSERT as idempotent success,
without target inspection, equality checks, replacement, or conflict evidence. A
skipped duplicate may leave divergent target contents; explicit broad
source-authoritative convergence uses the staged `sync` operation.

MySQL `1452` from an INSERT or UPDATE resolves the exact target constraint,
fetches the exact same-schema parent row from the source, recursively installs a
bounded parent chain inside the current target transaction, and retries the
blocked row. When an older native INSERT references a composite parent key that
no longer exists, CDC loads the current source child by primary key. It applies
the current row recursively when the FK values changed, skips the historical
INSERT when the source row is absent, and fails closed when the current row still
requests the missing parent. If a repair-generated parent insert hits `1062`, CDC
locks the exact duplicate-index owner in the same target transaction. It updates the owner
from source, or deletes a source-absent different-primary-key owner, then inserts
and verifies the intended parent before retrying the child. Ambiguous ownership,
unsupported index metadata, a remaining duplicate, or verification failure rolls
back without checkpoint advancement. Native source INSERT `1062` behavior remains
unchanged. Repair remains serial inside the active target transaction; existing
group-size and timeout controls may group complete source transactions, but no
concurrent target workers or parallel live-stream option are supported.

Conflict data remains out-of-band. Targeted conflict resolution connects to
source and target as a separate workflow from the live stream. The live stream
neither reads nor writes the conflict ledger and
does not validate its schema, procedures, or grants. Historical ledger
persistence is owned by concrete `MySqlConflictLedger` in
`src/conflict_ledger.rs` and `src/conflict_ledger/`; shared foreign-key
canonicalization is owned by `src/canonical_foreign_key.rs`. Live missing-parent
repair is independent from that ledger. General supersession and target-replacement
paths remain absent; the only source-current INSERT substitution is the narrow
missing-FK recovery described above.

The disposable MariaDB/MySQL harness covers DDL journal recovery,
reconnect/GET_LOCK behavior, serial grouped transaction boundaries, and
source-authoritative repair of production-shaped duplicate missing-FK parents
and historical child FK values superseded by current source rows. The nested
missing-FK proof runs through the serial live stream. These are local proofs,
not live cutover proof.

Deployment runs through `deploy.sh`, which checks the repository, publishes and
verifies an immutable image, scans it, and commits the ops image reference. Live
target execution is serial on one initialized `mysql::Conn`; source transactions
may still be grouped by the existing group-size and timeout controls, but no
parallel target-worker configuration exists. The legacy `probe` text-binlog path
is not a supported health check.

## DDL resolution

- [Authoritative DDL transformation spec](docs/specs/ddl-transformation.md)
- [DDL Resolution Runbook](docs/ddl-resolution.md) for journal barriers,
  translation-pending promotion, evidence inspection, and restart procedure.
- [Schema synchronization details](docs/specs/sync-schema.md) for source-to-target
  convergence through the shared streamed-DDL translator.

## Schema synchronization

The staged `sync` command is the only standalone synchronization entry point. It
runs prerequisite schema convergence, source-authoritative locked row chunks, and
final constraint convergence under one immutable run identity. Schema work is not
available as a separate `sync-schema` command; `sync-catalog`, `resync-stream`, and
`recover-lost-binlog` route through the same staged engine.

Before a potentially lossy column change, the prerequisite schema stage checks
actual target data. Values that would truncate, coerce, or fail block that table
and produce representative primary keys; independent tables continue, while
dependent operations are skipped. The staged run persists progress in
`cdc.sync_runs` and fails closed if final structural convergence is not achieved.

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

cargo run -- sync --source-host 127.0.0.1 --source-user reader \
  --source-password-env SOURCE_PASSWORD --source-database app \
  --target-host 127.0.0.1 --target-user writer \
  --target-password-env TARGET_PASSWORD --target-database app \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem \
  --table accounts --run-id accounts-sync-20260817-01

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

`sync` derives table columns and primary-key ordering from source inventory. Repeat
`--table` for a closed source scope and provide exactly one immutable `--run-id`
or `--run-id-prefix`; the progress table defaults to `cdc.sync_runs`. The command
runs prerequisite schema convergence, source-authoritative locked row chunks, and
final constraint convergence as one staged operation.

`--authorize-old-run-spec-sha256 <64-lowercase-hex>` is an explicit recovery-only
option for an exact `--run-id`. It authorizes migration from that one persisted
run-spec hash only when endpoints, settings, ordered scope, primary keys, primary-key
ordering, and existing writable-column order are unchanged; current source and
target schemas must agree, and each changed table must have no rows-stage progress.
The command locks and revalidates every run row in one serializable transaction,
updates only `run_spec_json`, verifies affected/current row counts, and rolls back
on any failure without retrying the transaction. A retry after commit is a no-write
`already_current` result. Authorization is not serialized into the run identity,
and it is not a general run-spec compatibility bypass.

The removed
`catchup-progress`, `sync-progress`, standalone `sync-schema`, and `drift-check`
commands are not available; their work is either part of `sync` stages or removed
from the CLI. Legacy `catchup-snapshot`, `sync-table`, and `repair-drift` names are
also rejected as unknown commands.

Live `stream-binlog` applies target work serially on one initialized
`mysql::Conn`. Existing target transaction group-size and timeout controls may
combine complete source transactions before one atomic target commit and
checkpoint write. There is no supported `--target-parallel-transactions` option
or concurrent target-worker path.

Run the disposable source-authoritative proofs with:

```sh
python3 scripts/cdc-integration-harness.py --scenario insert-duplicate-idempotent
python3 scripts/cdc-integration-harness.py --scenario missing-fk-parent-auto-insert
python3 scripts/cdc-integration-harness.py --scenario missing-fk-nested-parent-auto-insert
python3 scripts/cdc-integration-harness.py --scenario missing-fk-superseded-insert
python3 scripts/cdc-integration-harness.py --scenario sync-authorized-additive-spec-migration
```

The serial proofs cover divergent-target INSERT `1062` continuation, nested
missing-FK repair, source-current child substitution, and exact checkpoint
advancement. Out-of-band repair scenarios retain separate ledger and comparison
identities.

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

`sync-catalog` reads only the syncable catalog and invokes one unified sync run;
there is no dry-run/plan mode and `table-catalog` does not launch it. The command
waits until the unified staged run completes or fails. Every catalog table is
mapped into one `SyncConfig` with the configured source and target, ordered table
scope, chunk size, bounded catalog parallelism, progress table, and shared
non-empty `--run-id-prefix`. Unless `--progress-table` overrides it,
sync-catalog uses `cdc.sync_runs`. Unified sync derives one immutable run identity
and persists schema-stage, row-stage, and final-constraint progress there.

The unified run owns prerequisite schema convergence, locked source-authoritative
row chunks, bounded row workers, and final constraint convergence. The removed
catalog-specific dependency scheduler, admission locks, deterministic child run
IDs, target-only repair verification, and per-table progress handling are not
used. Catalog FK metadata still classifies syncable scope; it does not create
separate child runs. `resync-stream` now uses this unified path with one captured
source evidence set, a fixed `resync-stream:<source_identity>` run identity, and
`cdc.sync_runs` progress. It has no legacy repair phases or post-write
target-inventory drift scan.
`recover-lost-binlog` now uses the same staged engine with one captured source
evidence set, exact `recovery_id` progress across every source table, and
`cdc.sync_runs` progress. Prepared evidence is source-only. Recovery proof
requires complete exact run/table progress plus an unchanged source scope; it
does not capture a target final inventory or run a post-write drift scan. The stream lease, authorization,
checkpoint/barrier revalidation, and atomic recovery transaction remain in
place. This documents code behavior only; deployment and production execution
are not claimed. The non-syncable catalog is classification/operator input only;
full-dump execution is out of scope.

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

`--insert-conflict-policy` applies to statement replay. Values are `error`,
`ignore-duplicate`, and `replace-divergent-pk`:

- Generic target execution treats MySQL `1062` as success only for statements
  beginning with `INSERT INTO` under `ignore-duplicate`.
- Native ROW streaming does not use this policy. Its fixed rule accepts INSERT
  `1062`, attempts bounded source-authoritative repair for eligible `1452`, and
  fails every other row error.
- Unified sync is source-authoritative and uses strict insert/update/delete
  mutations; its staged progress defaults to `cdc.sync_runs`.

The default policy is `error`.

`--stop-position` is an inclusive event-end boundary: the event whose
`end_log_pos` equals the requested position is applied and durably checkpointed,
then the stream exits. A position inside an event, inside an open row transaction,
or not reached before EOF fails without partial-transaction completion.

The cross-engine inventory query reports `IS_VISIBLE='YES'` for index rows for
MariaDB compatibility. That value is not proof that a MySQL target index is
visible; inspect target-native visibility before admitting affected index DDL, or
leave it in the journal's translation-pending barrier.

See [Unified sync](docs/specs/unified-sync.md) for staged synchronization rules.
