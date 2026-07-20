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
conflicts still roll back without checkpoint advancement. The accepted overwrite
risk is explicit. If a later
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

## Recurring drift repair

`repair-drift` creates a fresh orchestration ID, inventories both endpoints, runs
bounded count/content checks, and invokes FK-aware phased `sync-table` repairs.
Dry-run is default. Apply mode requires an explicit `--max-deletes` value,
including zero when no orphan deletion is allowed. `--start-after`/`--end-at`
select a bounded primary-key window; JSON forms support comma-containing keys.

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
  --mode apply --max-deletes 25 --parent-first users,applications_users
```

Use this only after reviewing the exact target extras and FK constraints. Do not
assume lexical order can satisfy parent-first inserts and child-first deletes.

## Table repair runs

Every `sync-table` invocation requires `--run-id`. Direct reuse is limited to the
exact interrupted immutable run; a completed ID is terminal. A changed endpoint,
table shape, bounds, mode, delete ceiling, or `updated-since` specification needs
a fresh ID. During apply-mode InsertMissing, `repair-drift` may reclaim exactly
one failed missing-PK run only when its complete immutable specification matches;
multiple compatible candidates fail closed.

Apply mode preflights target extras before mutating. If extras exceed the
explicit ceiling, it performs zero inserts, updates, or deletes. Normal
`sync-table` repair never deletes or updates by a secondary key.

## Completion gates

- [ ] Durable live conflict observations and recurring repair scheduling.
- [x] FK-aware real-MySQL phased repair with crash/resume proof for the repaired scope.
- [x] Zero unresolved conflict debt after verified equality for the repaired scope.
- [ ] Stable checkpoint/lag, schema parity, and no quarantine/manual/journal debt.
