#!/usr/bin/env python3
"""Disposable end-to-end proof for the audited purged-binlog recovery CLI."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType


REPO = Path(__file__).resolve().parents[1]
HARNESS_PATH = REPO / "scripts" / "cdc-integration-harness.py"
RECOVERY_ID = "lost-binlog-harness-recovery-01"


def load_harness_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("cdc_integration_harness", HARNESS_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load harness helpers from {HARNESS_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


h = load_harness_module()
Harness = h.Harness
HarnessError = h.HarnessError
HarnessSkip = h.HarnessSkip
Coordinate = h.Coordinate
SOURCE_IDENTITY = h.SOURCE_IDENTITY
SOURCE_PASSWORD = h.SOURCE_PASSWORD
SOURCE_USER = h.SOURCE_USER
TARGET_PASSWORD = h.LIVE_TARGET_PASSWORD
TARGET_USER = h.LIVE_TARGET_USER
require_success = h.require_success
run = h.run
sql_literal = h.sql_literal


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, help="built mariadb-mysql-cdc binary")
    parser.add_argument("--keep", action="store_true", help="keep disposable containers on failure")
    return parser.parse_args()


def recovery_args(harness: Harness, binary: Path, authorization: Path) -> list[str]:
    assert harness.source and harness.target
    return [
        str(binary),
        "recover-lost-binlog",
        "--authorization-file",
        str(authorization),
        "--source-host",
        "127.0.0.1",
        "--source-port",
        str(harness.source.port),
        "--source-user",
        SOURCE_USER,
        "--source-password-env",
        "CDC_SOURCE_PASSWORD",
        "--source-database",
        "globalcomix",
        "--source-identity",
        SOURCE_IDENTITY,
        "--checkpoint-table",
        "cdc.stream_checkpoint",
        "--target-host",
        "127.0.0.1",
        "--target-port",
        str(harness.target.port),
        "--target-user",
        TARGET_USER,
        "--target-password-env",
        "CDC_TARGET_PASSWORD",
        "--target-database",
        "globalcomix",
        "--target-tls-ca-file",
        str(harness.ca_file),
    ]


def run_recovery(harness: Harness, authorization: Path) -> h.CommandResult:
    binary = harness._sync_binary()
    return run(
        recovery_args(harness, binary, authorization),
        cwd=REPO,
        env={
            **os.environ,
            "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD,
            "CDC_TARGET_PASSWORD": TARGET_PASSWORD,
        },
        timeout=240,
        check=False,
    )


def wait_for_pending_barrier(harness: Harness, process: subprocess.Popen[str]) -> dict[str, str]:
    assert harness.target
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        rows = harness.query(
            harness.target,
            "SELECT source_identity,binlog_file,event_start_position,event_end_position,raw_sql,status "
            "FROM cdc.ddl_replay_journal WHERE status='translation_pending' LIMIT 1;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).splitlines()
        if rows:
            fields = rows[0].split("\t")
            if len(fields) == 6:
                return dict(zip(("source_identity", "binlog_file", "start", "end", "raw_sql", "status"), fields, strict=True))
        if process.poll() is not None:
            raise HarnessError(f"stream exited before translation-pending journal write: {harness.process_output(process)}")
        time.sleep(0.2)
    raise HarnessError(f"stream did not persist translation-pending barrier: {harness.process_output(process)}")


def terminate_stream(harness: Harness, process: subprocess.Popen[str]) -> None:
    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired as error:
        process.kill()
        raise HarnessError("stream did not terminate after durable barrier capture") from error
    log = getattr(process, "_cdc_log", None)
    if log is not None:
        log.close()


def assert_checkpoint(harness: Harness, expected: dict[str, object]) -> None:
    actual = harness.checkpoint()
    if actual != expected:
        raise HarnessError(f"checkpoint changed unexpectedly: expected={expected!r} actual={actual!r}")


def assert_no_recovery_records(harness: Harness) -> None:
    assert harness.target
    count = harness.admin_query(harness.target, "SELECT COUNT(*) FROM cdc.stream_recovery_records;").strip()
    if count != "0":
        raise HarnessError(f"rejected authorization created recovery state: records={count}")


def write_authorization(path: Path, checkpoint: dict[str, object], barrier: dict[str, str], *, mismatch: bool) -> None:
    expected_checkpoint = dict(checkpoint)
    if mismatch:
        expected_checkpoint["source_position"] = int(expected_checkpoint["source_position"]) + 1
    request = {
        "recovery_id": RECOVERY_ID if not mismatch else f"{RECOVERY_ID}-mismatch",
        "checkpoint_name": f"stream-binlog:{SOURCE_IDENTITY}",
        "expected_checkpoint": expected_checkpoint,
        "expected_barrier": {
            "source_identity": barrier["source_identity"],
            "binlog_file": barrier["binlog_file"],
            "event_start_position": int(barrier["start"]),
            "event_end_position": int(barrier["end"]),
            "raw_sql": barrier["raw_sql"],
        },
        "operator_identity": "lost-binlog-harness@example.test",
        "reason": "disposable purged-history recovery proof",
    }
    path.write_text(json.dumps(request, separators=(",", ":")))


def purge_checkpoint_history(harness: Harness, checkpoint: Coordinate) -> None:
    assert harness.source
    for _ in range(3):
        harness.admin_sql(harness.source, "FLUSH BINARY LOGS;")
        time.sleep(0.2)
    retained = harness.coordinate()
    if retained.file == checkpoint.file:
        raise HarnessError(f"source did not rotate after FLUSH BINARY LOGS: {retained}")
    for _ in range(10):
        harness.admin_sql(harness.source, f"PURGE BINARY LOGS TO {sql_literal(retained.file)};")
        logs = harness.admin_query(harness.source, "SHOW BINARY LOGS;")
        if checkpoint.file not in logs:
            return
        time.sleep(0.2)
    raise HarnessError(f"checkpoint binlog remained after purge: checkpoint={checkpoint.file} logs={logs!r}")


def assert_reconciled_rows(harness: Harness) -> None:
    assert harness.target
    accounts = harness.admin_query(harness.target, "SELECT id,payload FROM accounts ORDER BY id;").strip()
    if accounts != "1\tsource-current":
        raise HarnessError(f"source-authoritative accounts reconciliation failed: {accounts!r}")
    generated = harness.admin_query(harness.target, "SELECT id,base,doubled FROM generated_values ORDER BY id;").strip()
    if generated != "1\t7\t14":
        raise HarnessError(f"generated-column reconciliation failed: {generated!r}")


def assert_committed_transition(harness: Harness, checkpoint: dict[str, object], barrier: dict[str, str]) -> None:
    assert harness.target
    row = harness.admin_query(
        harness.target,
        "SELECT status,old_checkpoint_json,old_barrier_file,old_barrier_start_position,"
        "old_barrier_end_position,old_barrier_raw_sql FROM cdc.stream_recovery_records "
        f"WHERE recovery_id={sql_literal(RECOVERY_ID)};",
    ).strip().split("\t", 5)
    if len(row) != 6:
        raise HarnessError(f"missing committed recovery record: {row!r}")
    status, old_checkpoint_json, file, start, end, raw_sql = row
    if (status, json.loads(old_checkpoint_json), file, start, end, raw_sql) != (
        "committed",
        checkpoint,
        barrier["binlog_file"],
        barrier["start"],
        barrier["end"],
        barrier["raw_sql"],
    ):
        raise HarnessError(f"recovery record did not bind historical identity: {row!r}")
    journal = harness.admin_query(
        harness.target,
        "SELECT status FROM cdc.ddl_replay_journal WHERE source_identity="
        f"{sql_literal(barrier['source_identity'])} AND binlog_file={sql_literal(barrier['binlog_file'])} "
        f"AND event_start_position={barrier['start']} AND event_end_position={barrier['end']} "
        f"AND raw_sql={sql_literal(barrier['raw_sql'])};",
    ).strip()
    if journal != "translation_pending":
        raise HarnessError(f"historical journal barrier was not preserved: {journal!r}")


def run_scenario(binary: Path | None, keep: bool) -> None:
    with Harness(REPO, binary, keep) as harness:
        harness.prepare()
        assert harness.source and harness.target
        harness.admin_sql(
            harness.source,
            "CREATE TABLE accounts (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL, "
            "KEY idx_payload (payload)) ENGINE=InnoDB;"
            "CREATE TABLE generated_values (id BIGINT NOT NULL PRIMARY KEY, base INT NOT NULL, "
            "doubled INT GENERATED ALWAYS AS (base * 2) STORED) ENGINE=InnoDB;"
            "INSERT INTO accounts VALUES (1,'source-current');"
            "INSERT INTO generated_values (id,base) VALUES (1,7);",
        )
        harness.admin_sql(
            harness.target,
            "CREATE TABLE accounts (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL, "
            "KEY idx_payload (payload)) ENGINE=InnoDB;"
            "CREATE TABLE generated_values (id BIGINT NOT NULL PRIMARY KEY, base INT NOT NULL, "
            "doubled INT GENERATED ALWAYS AS (base * 2) STORED) ENGINE=InnoDB;"
            "INSERT INTO accounts VALUES (1,'target-divergent'),(2,'target-only');"
            "INSERT INTO generated_values (id,base) VALUES (1,1),(2,2);"
            "GRANT LOCK TABLES ON globalcomix.* TO 'cdc_stream'@'%';"
            "GRANT CREATE ON cdc.* TO 'cdc_stream'@'%';"
            "GRANT SELECT,INSERT,UPDATE ON cdc.sync_runs TO 'cdc_stream'@'%';",
        )
        harness.admin_sql_file(harness.target, REPO / "docs/stream-recovery-records-bootstrap.sql")

        start = harness.coordinate()
        harness.write_checkpoint(start)
        checkpoint = harness.checkpoint()
        harness.admin_sql(harness.source, "ALTER TABLE accounts RENAME INDEX idx_payload TO idx_payload_renamed;")
        blocked, _log = harness.start_stream(start, label="lost-binlog-blocked")
        barrier = wait_for_pending_barrier(harness, blocked)
        terminate_stream(harness, blocked)
        assert_checkpoint(harness, checkpoint)
        purge_checkpoint_history(harness, start)

        mismatch = harness.tempdir / "recovery-mismatch.json"
        write_authorization(mismatch, checkpoint, barrier, mismatch=True)
        rejected = run_recovery(harness, mismatch)
        if rejected.returncode == 0 or "checkpoint" not in f"{rejected.stdout}\n{rejected.stderr}".lower():
            raise HarnessError(f"mismatched recovery authorization was accepted: {rejected}")
        assert_checkpoint(harness, checkpoint)
        assert_no_recovery_records(harness)

        authorization = harness.tempdir / "recovery.json"
        write_authorization(authorization, checkpoint, barrier, mismatch=False)
        recovered = run_recovery(harness, authorization)
        require_success(recovered, "lost-binlog recovery")
        report = json.loads(recovered.stdout)
        if report.get("recovery_id") != RECOVERY_ID or report.get("compared_tables") != 2:
            raise HarnessError(f"unexpected recovery report: {report!r}")
        assert_reconciled_rows(harness)
        assert_committed_transition(harness, checkpoint, barrier)
        durable_recovery_checkpoint = harness.checkpoint()
        if durable_recovery_checkpoint["source_file"] == start.file:
            raise HarnessError(f"recovery did not advance beyond purged checkpoint: {durable_recovery_checkpoint!r}")

        harness.admin_sql(harness.source, "INSERT INTO accounts VALUES (3,'post-recovery');")
        stop = harness.coordinate()
        resumed = harness.run_stream(Coordinate(str(durable_recovery_checkpoint["source_file"]), int(durable_recovery_checkpoint["source_position"])), stop)
        require_success(resumed, "post-recovery stream restart")
        rows = harness.admin_query(harness.target, "SELECT id,payload FROM accounts ORDER BY id;").strip()
        if rows != "1\tsource-current\n3\tpost-recovery":
            raise HarnessError(f"post-recovery DML was not streamed: {rows!r}")
        final_checkpoint = harness.checkpoint()
        if final_checkpoint["source_file"] != stop.file or int(final_checkpoint["source_position"]) != stop.position:
            raise HarnessError(f"stream did not advance checkpoint after recovery: expected={stop} actual={final_checkpoint!r}")
        print(
            "lost_binlog_recovery_ok "
            f"recovery_id={RECOVERY_ID} repaired_tables={report['repaired_tables']} "
            f"checkpoint={stop.file}:{stop.position}"
        )


def main() -> int:
    args = parse_args()
    try:
        run_scenario(args.binary, args.keep)
    except HarnessSkip as error:
        print(f"harness_skip prerequisite={error}")
        return 0
    except HarnessError as error:
        print(f"harness_error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
