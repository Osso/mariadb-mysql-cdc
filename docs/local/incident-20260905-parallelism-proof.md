# Lost-binlog recovery parallelism proof

Verified: 2026-09-06
Revision: `eabf1b0`

## Scope

`recover-lost-binlog` and `resume-lost-binlog` accept `--parallelism WORKERS`.
The default remains `1`. The selected worker count only populates the unified
sync configuration; recovery ID, prepared boundary, and durable progress are
unchanged.

## TDD

RED:

```text
cargo test --bin mariadb-mysql-cdc lost_binlog_unified
error[E0609]: no field `parallelism` on type `RecoverLostBinlogConfig`
```

GREEN:

```text
cargo test --bin mariadb-mysql-cdc lost_binlog_unified
4 passed; 0 failed

cargo test --test lost_binlog_resume_cli
4 passed; 0 failed

cargo fmt --check
exit=0
```

The CLI test executes both commands and proves help advertises the flag plus
`0` and non-integer values fail before authorization parsing or connections.
The unit test proves default `1`, explicit `2`, and propagation into the unified
sync configuration.

Not covered here: interrupted `1 -> 2` resume and observed multi-worker
concurrency; owned by the parent integration proof.
