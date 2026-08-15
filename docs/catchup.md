# Catchup Workflow

Catchup uses a snapshot plus forward CDC replay:

1. Record a binlog boundary.
2. Snapshot source tables in primary-key chunks.
3. Replay events from that boundary in binlog order.
4. Continue streaming to the live tail.
5. Validate counts, stable primary-key windows, content, checksums, orphans, and
   schema state.

This is not a parity proof by itself. `INSERT IGNORE` preserves any conflicting
target row, but does not overwrite divergent values or remove target orphans.
Snapshot `INSERT IGNORE` is independent of `--insert-conflict-policy`.
For a native live ROW `INSERT`, `ignore-duplicate` skips MySQL `1062` only when
the target row fetched by source primary key exactly equals the source row.
The explicit `replace-divergent-pk` policy may replace an unequal row only for a
`PRIMARY` duplicate after an exactly-one-row PK lookup and exactly-one-row
primary-key UPDATE match; durable audit evidence records the decision. Missing or
multiple PK rows and secondary-unique, foreign-key, CHECK, or replacement-update
conflicts still roll back without checkpoint advancement. The live stream has
superseded historical exceptions for `globalcomix.users`/`users.name`,
`globalcomix.comics`/`comics.slug`, and two exact `globalcomix.releases` FK
transactions: category `mysqld-bin.002709:515816736–515824875`
(`releases_ibfk_2`) and visibility `mysqld-bin.002709:531921570–531929925`
(`releases_ibfk_3`, candidate event `531921789`). Each allows exactly one
deferred candidate and rejects any mixed ordinary conflict. It reads `SHOW MASTER STATUS` before
one `START TRANSACTION WITH CONSISTENT SNAPSHOT`; that pre-snapshot coordinate
is a conservative lower bound and must be beyond the candidate transaction. The
users proof requires consistent-source full-row and active-transaction target
`FOR UPDATE` proof for both historical PK and current unique owner. The comics
proof requires full current primary-row equality, while accepting the locked
unique owner by exact PK+slug identity despite unrelated mutable-field drift. If
typed verification finds that the source primary still owns the historical identity,
it records ordinary unresolved reconciliation debt, runs no superseded repair SQL,
and commits the remaining transaction with its XID checkpoint; other proof or
evidence failures still roll back. For releases, the exact FK child/parent identity must match the approved
transaction; the complete historical release image is retained, later source
history must show a changed parent value, and exactly one current source release,
source parent, and locked target parent identity must match. An absent target
release gets the complete current source row installed; an existing target
release must hash equal to current source. The parent identity is preserved and
never updated or deleted. Before writing the XID checkpoint, the target
transaction requires an existing same-file predecessor before the candidate and
no later than the XID. Remaining rows in that source
transaction still apply; any other proof, predecessor, or commit failure rolls
back, then persists all unresolved observations independently, surfacing rollback or
persistence failures. Success commits its exact observation/resolution evidence
and XID checkpoint atomically. The accepted overwrite risk is explicit. If a later
conflict rolls back the enclosing target transaction, the replacement rolls back
while its independent ledger evidence remains. The default `error` policy also
rolls back on the duplicate.

Catchup source reads and target writes follow the [connection policy](schema-inventory.md#connection-policy).
The live source MariaDB (`source-mariadb.example` / `192.0.2.10`) is
plaintext-only; `catchup-snapshot`, `sync-table`, and `repair-drift` must not
require or pass a source CA for that source. Target writes continue to require
the reviewed DigitalOcean CA at `/etc/mariadb-mysql-cdc/do-ca.pem` plus target
hostname verification. The deployment must keep target CA material mounted while
omitting source CA arguments for the current source. The second successful
catchup invocation proves a completed-run no-op, not an interrupted
parallel-range resume.

For a resumable snapshot backfill:

```bash
mariadb-mysql-cdc catchup-snapshot \
  --source-host 192.0.2.10 \
  --source-user cdc_reader \
  --source-password-env SOURCE_PASSWORD \
  --source-database globalcomix \
  --target-host target-mysql.example \
  --target-port 25060 \
  --target-user target_user \
  --target-password-env TARGET_PASSWORD \
  --target-database globalcomix \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem \
  --progress-file /var/lib/mariadb-mysql-cdc/snapshot-progress.json \
  --chunk-size 10000 \
  --parallel-workers 4
```

## Progress-table privilege boundary

Repair commands default to `cdc.table_sync_runs`. Runtime `ensure` validates
its complete 16-column schema and `run_id` primary key through
`information_schema`; an administrator-prebootstrapped current table therefore
needs only `SELECT, INSERT, UPDATE` at runtime. An exact legacy 15-column table
is migrated once by adding the `delete_preflight_complete` column as
`BOOLEAN NOT NULL DEFAULT FALSE`, so that upgrade path requires `ALTER` and
preserves existing runs as requiring preflight. A missing table uses the
creation path and a malformed existing table fails unchanged.

`catchup-snapshot` defaults to the legacy `cdc.table_sync_progress` table. Its
current ensure path still creates the schema/table and conditionally adds
`total_rows`, so prebootstrapping this table does not yet make catchup
least-privilege. Do not claim catchup support with a table-DML-only runtime
account until that path is changed and proved.

## Recurring drift repair

`repair-drift` creates a fresh orchestration ID, inventories both endpoints, runs
bounded count/content checks, and invokes FK-aware phased `sync-table` repairs.
Dry-run is default. Apply mode reconciles target-only rows in dependency-safe
chunks, verifies each chunk, and persists progress after verification.
`--start-after`/`--end-at` select a bounded primary-key window; JSON forms support
comma-containing keys.

The planner deletes child rows before parents, inserts parents before children,
blocks cycles/schema mismatch before mutation, rejects changed plan hashes on
resume, and resolves conflict rows only after verified equality with evidence.

```bash
mariadb-mysql-cdc repair-drift \
  --source-host 192.0.2.10 --source-user cdc_reader \
  --source-password-env SOURCE_PASSWORD --source-database globalcomix \
  --target-host target-mysql.example \
  --target-port 25060 --target-user target_user \
  --target-password-env TARGET_PASSWORD --target-database globalcomix \
  --target-tls-ca-file /etc/mariadb-mysql-cdc/do-ca.pem \
  --mode apply --parent-first users,applications_users
```

Use this only after reviewing the exact target extras and FK constraints. Do not
assume lexical order can satisfy parent-first inserts and child-first deletes.

## Table repair runs

Every `sync-table` invocation requires `--run-id`. Direct reuse is limited to the
exact interrupted immutable run; a completed ID is terminal. A changed endpoint, table shape, bounds, mode, or `updated-since` specification
needs a fresh ID. During apply-mode InsertMissing, `repair-drift` may reclaim exactly
one failed missing-PK run only when its complete immutable specification matches.
Reclamation is an atomic claim scoped to the table and immutable specification:
compatibility and uniqueness are revalidated before the selected row is marked
running; multiple compatible candidates fail closed without reclaiming a run.
The claim acquires a table/specification-scoped `GET_LOCK`, then uses a target
transaction explicitly set to `REPEATABLE READ`; candidate enumerations use
`FOR UPDATE`, the selected row is marked `running` and committed, and only then
is the advisory lock released. Any failure rolls back the selection transaction
before lock release.

Apply mode preflights target extras before mutating. For `All` and
`DeleteExtras`, a successful preflight sets `delete_preflight_complete` on the
run row before repair starts; reconnects skip that completed scan, while failed
or incomplete preflights remain unmarked and rerun. If extras exceed the
explicit ceiling, it performs zero inserts, updates, or deletes. Normal
`sync-table` repair never deletes or updates by a secondary key.

## Completion gates

- [ ] Durable live conflict observations and recurring repair scheduling.
- [x] FK-aware real-MySQL phased repair with crash/resume proof for the repaired scope.
- [x] Zero unresolved conflict debt after verified equality for the repaired scope.
- [ ] Stable checkpoint/lag, schema parity, and no quarantine/manual/journal debt.
