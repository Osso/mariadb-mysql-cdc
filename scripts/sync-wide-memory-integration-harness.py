#!/usr/bin/env python3
"""Replay a source-wide OOM and same-prepared recovery under a 2 GiB cap."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "wide_memory_helpers", REPO / "scripts/sync-memory-integration-harness.py"
)
assert SPEC and SPEC.loader
mem = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mem
SPEC.loader.exec_module(mem)
TABLES = ("memory_rows", "memory_rows_peer")
BODY_BYTES = 104_878
RECOVERY_ID = "wide-byte-page-recovery"
BASELINE_SHA = "6b49883d35013a951fc7e470023399d5795534c78f50ce8278e7f4f36531b023"


def body_expression(identifier: str, prefix: str = "source") -> str:
    return f"RPAD(CONCAT('{prefix}-',{identifier}),{BODY_BYTES},'x')"


def seed(harness: object) -> None:
    for endpoint in (harness.source, harness.target):
        for table in TABLES:
            harness.admin_sql(
                endpoint,
                f"CREATE TABLE {table} (id BIGINT PRIMARY KEY,body MEDIUMTEXT NOT NULL,"
                "kind VARCHAR(16) NOT NULL,KEY idx_kind(kind)) ENGINE=InnoDB;",
            )
            prefix = mem.numbered_rows(20_000, 5)
            harness.admin_sql(
                endpoint,
                f"INSERT INTO {table} SELECT id,CONCAT('small-',id),'small' "
                f"FROM ({prefix}) numbers;",
            )
            for offset in range(0, 10_000, 1_000):
                numbers = mem.numbered_rows(1_000, 20_005 + offset)
                label = (
                    "stale"
                    if endpoint == harness.target and table.endswith("peer")
                    else "source"
                )
                harness.admin_sql(
                    endpoint,
                    f"INSERT INTO {table} SELECT id,{body_expression('id', label)},'wide' "
                    f"FROM ({numbers}) numbers;",
                )
    harness.admin_sql(
        harness.target,
        "DELETE FROM memory_rows_peer WHERE id=30005; "
        f"INSERT INTO memory_rows_peer VALUES(30006,{body_expression('30006', 'extra')},'wide');",
    )


def prepare(harness: object) -> tuple[Path, dict, dict]:
    seed(harness)
    start = harness.coordinate()
    harness.write_checkpoint(start)
    checkpoint = harness.checkpoint()
    harness.admin_sql(
        harness.source,
        "ALTER TABLE memory_rows RENAME INDEX idx_kind TO idx_kind_recovery;",
    )
    stream, _ = harness.start_stream(start, label="wide-memory-barrier")
    barrier = mem.lost.wait_for_pending_barrier(harness, stream)
    mem.lost.terminate_stream(harness, stream)
    harness.admin_sql(
        harness.target,
        "ALTER TABLE memory_rows RENAME INDEX idx_kind TO idx_kind_recovery;",
    )
    mem.lost.purge_checkpoint_history(harness, start)
    authorization = harness.tempdir / "wide-authorization.json"
    mem.lost.RECOVERY_ID = RECOVERY_ID
    mem.lost.write_authorization(authorization, checkpoint, barrier, mismatch=False)
    return authorization, checkpoint, barrier


def run_worker(
    harness: object,
    binary: Path,
    authorization: Path,
    evidence: Path,
    label: str,
    resume: bool,
) -> object:
    name = f"cdc-wide-memory-{label}-{os.getpid()}"
    directory = mem.make_worker_input(evidence, name, authorization, harness.ca_file)
    command = mem.worker_command(harness, binary, authorization, directory)
    position = command.index("recover-lost-binlog")
    command[position] = "resume-lost-binlog" if resume else "recover-lost-binlog"
    command[position + 1 : position + 1] = ["--parallelism", "2"]
    try:
        launched = mem.run(command, check=False, timeout=1800)
        result = mem.read_worker_result(name)
        (evidence / f"{label}.json").write_text(
            json.dumps(
                {
                    "binary": str(binary),
                    "sha256": mem.sha256(binary),
                    "docker_exit": launched.returncode,
                    "exit_code": result.exit_code,
                    "oom_killed": result.oom_killed,
                    "inspect": result.inspect,
                    "log": result.log,
                },
                indent=2,
            )
        )
        return result
    finally:
        mem.run(["docker", "rm", "-f", name], check=False)


def progress(harness: object) -> str:
    return harness.admin_query(
        harness.target,
        "SELECT table_name,status,last_primary_key_json,rows_scanned FROM cdc.sync_runs "
        f"WHERE run_id='{RECOVERY_ID}' AND stage='rows' ORDER BY table_name;",
    ).strip()


def prepared_record(harness: object) -> str:
    return harness.admin_query(
        harness.target,
        "SELECT recovery_id,status,new_checkpoint_json FROM cdc.stream_recovery_records "
        f"WHERE recovery_id='{RECOVERY_ID}';",
    ).strip()


def assert_equal_fixture(harness: object) -> None:
    for table in TABLES:
        expression = f"IF(id<=20005,CONCAT('small-',id),{body_expression('id')})"
        actual = harness.admin_query(
            harness.target,
            f"SELECT COUNT(*),SUM(body<>{expression}),SUM(id=30005),SUM(id=30006) FROM {table};",
        ).strip()
        if actual != "30000\t0\t1\t0":
            raise mem.HarnessError(f"wide fixture mismatch table={table}: {actual!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    baseline = mem.require_binary(args.baseline, "baseline")
    if mem.sha256(baseline) != BASELINE_SHA:
        raise mem.HarnessError("baseline does not match the pre-byte-budget runtime")
    candidate = (
        mem.require_binary(args.candidate, "candidate") if args.candidate else None
    )
    evidence = args.evidence_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    evidence.mkdir(parents=True)
    with mem.Harness(REPO, baseline, False) as harness:
        harness.prepare()
        authorization, checkpoint, barrier = prepare(harness)
        old = run_worker(harness, baseline, authorization, evidence, "baseline", False)
        if old.exit_code != 137 or not old.oom_killed:
            raise mem.HarnessError(
                f"expected cgroup OOM: exit={old.exit_code} oom={old.oom_killed} log={old.log}"
            )
        expected = "\n".join(f'{table}\trunning\t["20005"]\t20000' for table in TABLES)
        if progress(harness) != expected or harness.checkpoint() != checkpoint:
            raise mem.HarnessError(
                f"OOM did not preserve prefix/checkpoint: {progress(harness)!r}"
            )
        prepared = prepared_record(harness)
        (evidence / "prepared-before-resume.txt").write_text(prepared)
        print(
            f"wide_memory_red_ok cap=2GiB workers=2 progress={progress(harness)!r}",
            flush=True,
        )
        if candidate is None:
            return 0
        harness.admin_sql(
            harness.target,
            "DELIMITER //\nCREATE TRIGGER wide_retry_failure BEFORE UPDATE ON memory_rows_peer FOR EACH ROW\n"
            "BEGIN IF NEW.id=20500 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='wide rollback proof'; END IF; END//\nDELIMITER ;\n",
        )
        failed = run_worker(
            harness, candidate, authorization, evidence, "rollback", True
        )
        if (
            failed.exit_code == 0
            or failed.oom_killed
            or "wide rollback proof" not in failed.log
        ):
            raise mem.HarnessError(f"expected strict rollback, not OOM: {failed.log}")
        unchanged = harness.admin_query(
            harness.target,
            f"SELECT COUNT(*),SUM(body<>{body_expression('id', 'stale')}) "
            "FROM memory_rows_peer WHERE id BETWEEN 20006 AND 20500;",
        ).strip()
        if (
            unchanged != "495\t0"
            or prepared_record(harness) != prepared
            or harness.checkpoint() != checkpoint
        ):
            raise mem.HarnessError(
                f"wide rollback changed rows/prepared/checkpoint: {unchanged!r}"
            )
        rows = progress(harness).splitlines()
        if rows[1] != 'memory_rows_peer\trunning\t["20005"]\t20000':
            raise mem.HarnessError(f"wide rollback advanced failed cursor: {rows!r}")
        (evidence / "progress-after-rollback.txt").write_text(progress(harness))
        harness.admin_sql(harness.target, "DROP TRIGGER wide_retry_failure;")
        complete = run_worker(
            harness, candidate, authorization, evidence, "resume", True
        )
        if complete.exit_code or complete.oom_killed:
            raise mem.HarnessError(f"candidate failed: {complete.log}")
        assert_equal_fixture(harness)
        mem.lost.assert_committed_transition(harness, checkpoint, barrier)
        (evidence / "progress-final.txt").write_text(progress(harness))
        print(
            f"wide_memory_green_ok cap=2GiB workers=2 rows=60000 rollback=true same_prepared=true evidence={evidence}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
