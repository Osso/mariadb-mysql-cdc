# Catchup Workflow

Catchup uses a snapshot plus forward CDC replay:

1. Record a binlog boundary.
2. Snapshot source tables in primary-key chunks.
3. Replay events from that boundary in binlog order.
4. Continue streaming to the live tail.
5. Validate counts, stable primary-key windows, content, checksums, orphans, and
   schema state.

This is not a parity proof by itself. `INSERT IGNORE` preserves target rows that
CDC already wrote, but does not overwrite divergent values or remove target
orphans. A skipped live duplicate remains reconciliation debt.

Target writes require the reviewed DigitalOcean CA path when mounted; a
missing, unreadable, or invalid target CA fails before the driver connects with an
endpoint-specific diagnostic. The catchup source SQL reader currently uses
non-TLS connections and has no source-CA verification path. The Kubernetes
catchup manifest does not mount the target CA; do not call a catchup run
CA-verified until that live deployment gap is closed.

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

Every `sync-table` invocation requires `--run-id`. Reuse an ID only for the exact
interrupted immutable run; a completed ID is terminal. A changed endpoint,
table shape, bounds, mode, delete ceiling, or `updated-since` specification needs
a fresh ID.

Apply mode preflights target extras before mutating. If extras exceed the
explicit ceiling, it performs zero inserts, updates, or deletes. Normal
`sync-table` repair never deletes or updates by a secondary key.

## Completion gates

- [ ] Durable live conflict observations and recurring repair scheduling.
- [x] FK-aware real-MySQL phased repair with crash/resume proof for the repaired scope.
- [x] Zero unresolved conflict debt after verified equality for the repaired scope.
- [ ] Stable checkpoint/lag, schema parity, and no quarantine/manual/journal debt.
