#!/usr/bin/env python3
"""Prove bounded lost-binlog target reconciliation in a real 2 GiB cgroup.

Creates only disposable MariaDB/MySQL containers. The old a73919c binary must
be OOM-killed after durable prefix progress; a candidate must reconcile the
same sparse target gap under the identical Docker resource cap.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

REPO = Path(__file__).resolve().parents[1]
HARNESS_PATH = REPO / "scripts" / "cdc-integration-harness.py"
LOST_BINLOG_PATH = REPO / "scripts" / "lost-binlog-integration-harness.py"
BASELINE_BINARY = Path("/tmp/claude/cdc-memory-baseline-a73919c")
BASELINE_SHA256 = "f5653b892c41df816f56251d6babe7fa0e841ecc10b79b943d7a6255c0caae27"
WORKER_IMAGE = "registry.digitalocean.com/globalcomix/mariadb-mysql-cdc@sha256:0457202fd70bbc244ba4ad82343aed73feab4375d499014fb18c437fd314370c"
RECOVERY_ID_PREFIX = "sync-memory-harness"
SMALL_ROWS = 60_000
SOURCE_LARGE_FIRST_ROWS = 1_000
SOURCE_LARGE_LAST_ROWS = 9_000
TARGET_LARGE_ROWS = 164_764
TARGET_INSERT_BATCH_ROWS = 5_000
LARGE_BODY_BYTES = 13_312
MEMORY_LIMIT = "2g"


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load helpers from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


h = load_module("cdc_memory_harness", HARNESS_PATH)
lost = load_module("cdc_memory_lost_binlog", LOST_BINLOG_PATH)
Harness = h.Harness
HarnessError = h.HarnessError
HarnessSkip = h.HarnessSkip
Coordinate = h.Coordinate
SOURCE_IDENTITY = h.SOURCE_IDENTITY
SOURCE_PASSWORD = h.SOURCE_PASSWORD
SOURCE_USER = h.SOURCE_USER
TARGET_PASSWORD = h.LIVE_TARGET_PASSWORD
TARGET_USER = h.LIVE_TARGET_USER
run = h.run
sql_literal = h.sql_literal


@dataclass(frozen=True)
class WorkerResult:
    name: str
    exit_code: int
    oom_killed: bool
    error: str
    log: str
    inspect: dict[str, object]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidate",
        type=Path,
        help="candidate mariadb-mysql-cdc binary built from bounded-page source",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=BASELINE_BINARY,
        help="immutable a73919c baseline binary",
    )
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        default=Path("/tmp/claude/cdc-memory-harness"),
        help="persistent log and resource-evidence directory",
    )
    parser.add_argument("--keep", action="store_true", help="keep database containers")
    parser.add_argument(
        "--baseline-only",
        action="store_true",
        help="record only the expected a73919c cgroup OOM RED result",
    )
    parser.add_argument(
        "--candidate-only",
        action="store_true",
        help="run candidate GREEN after a separately recorded identical-cap RED",
    )
    args = parser.parse_args()
    if args.baseline_only and args.candidate_only:
        parser.error("--baseline-only and --candidate-only are mutually exclusive")
    return args


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require_binary(path: Path, label: str) -> Path:
    if not path.is_file() or not os.access(path, os.X_OK):
        raise HarnessError(f"{label} binary is not executable: {path}")
    return path.resolve()


def verify_baseline(path: Path) -> Path:
    path = require_binary(path, "baseline")
    actual = sha256(path)
    if actual != BASELINE_SHA256:
        raise HarnessError(
            f"baseline SHA-256 mismatch: expected={BASELINE_SHA256} actual={actual}"
        )
    return path


def digits_expression() -> str:
    digits = " UNION ALL ".join(f"SELECT {value} AS digit" for value in range(10))
    return " CROSS JOIN ".join(f"({digits}) d{index}" for index in range(6))


def numbered_rows(limit: int, offset: int = 0) -> str:
    number = " + ".join(f"d{index}.digit * {10**index}" for index in range(6))
    return (
        f"SELECT {number} + {offset + 1} AS id FROM {digits_expression()} "
        f"WHERE {number} < {limit}"
    )


def large_body(id_sql: str, suffix: str = "") -> str:
    return f"RPAD(CONCAT('large-', {id_sql}, {sql_literal(suffix)}), {LARGE_BODY_BYTES}, 'x')"


def fixture_schema() -> str:
    return (
        "CREATE TABLE memory_rows ("
        "id BIGINT NOT NULL PRIMARY KEY,"
        "body MEDIUMTEXT NOT NULL,"
        "kind VARCHAR(16) NOT NULL,"
        "KEY idx_kind (kind)"
        ") ENGINE=InnoDB;"
    )


def seed_source(harness: Harness) -> None:
    assert harness.source
    last_offset = SMALL_ROWS + TARGET_LARGE_ROWS - SOURCE_LARGE_LAST_ROWS
    harness.admin_sql(
        harness.source,
        fixture_schema() + "INSERT INTO memory_rows (id,body,kind) "
        "SELECT seq,CONCAT('small-',seq),'small' FROM seq_1_to_60000;"
        + "INSERT INTO memory_rows (id,body,kind) "
        f"SELECT seq + {SMALL_ROWS},{large_body(f'seq + {SMALL_ROWS}')},'large' "
        "FROM seq_1_to_1000;" + "INSERT INTO memory_rows (id,body,kind) "
        f"SELECT seq + {last_offset},{large_body(f'seq + {last_offset}')},'large' "
        "FROM seq_1_to_9000;",
    )


def insert_target_rows(harness: Harness, count: int, offset: int, kind: str) -> None:
    assert harness.target
    rows = numbered_rows(count, offset)
    body = "CONCAT('small-',id)" if kind == "small" else large_body("id")
    harness.admin_sql(
        harness.target,
        "INSERT INTO memory_rows (id,body,kind) "
        f"SELECT id,{body},{sql_literal(kind)} FROM ({rows}) AS generated_numbers;",
    )


def seed_target(harness: Harness) -> None:
    assert harness.target
    harness.admin_sql(harness.target, fixture_schema())
    insert_target_rows(harness, SMALL_ROWS, 0, "small")
    for offset in range(0, TARGET_LARGE_ROWS, TARGET_INSERT_BATCH_ROWS):
        insert_target_rows(
            harness,
            min(TARGET_INSERT_BATCH_ROWS, TARGET_LARGE_ROWS - offset),
            SMALL_ROWS + offset,
            "large",
        )
    harness.admin_sql(
        harness.target,
        "UPDATE memory_rows SET body='target-stale-small' WHERE id=1;"
        "UPDATE memory_rows SET body='target-stale-large' WHERE id=60001;"
        "GRANT LOCK TABLES ON globalcomix.* TO 'cdc_stream'@'%';"
        "GRANT CREATE ON cdc.* TO 'cdc_stream'@'%';"
        "GRANT SELECT,INSERT,UPDATE ON cdc.sync_runs TO 'cdc_stream'@'%';",
    )


def prepare_recovery(
    harness: Harness, recovery_id: str
) -> tuple[Path, dict[str, object], dict[str, str]]:
    assert harness.source and harness.target
    seed_source(harness)
    seed_target(harness)
    start = harness.coordinate()
    harness.write_checkpoint(start)
    checkpoint = harness.checkpoint()
    harness.admin_sql(
        harness.source,
        "ALTER TABLE memory_rows RENAME INDEX idx_kind TO idx_kind_recovery;",
    )
    blocked, _log = harness.start_stream(start, label=f"{recovery_id}-barrier")
    barrier = lost.wait_for_pending_barrier(harness, blocked)
    lost.terminate_stream(harness, blocked)
    harness.admin_sql(
        harness.target,
        "ALTER TABLE memory_rows RENAME INDEX idx_kind TO idx_kind_recovery;",
    )
    lost.purge_checkpoint_history(harness, start)
    authorization = harness.tempdir / f"{recovery_id}.json"
    original_recovery_id = lost.RECOVERY_ID
    try:
        lost.RECOVERY_ID = recovery_id
        lost.write_authorization(authorization, checkpoint, barrier, mismatch=False)
    finally:
        lost.RECOVERY_ID = original_recovery_id
    return authorization, checkpoint, barrier


def worker_command(
    harness: Harness, binary: Path, authorization: Path, worker_dir: Path
) -> list[str]:
    assert harness.source and harness.target
    return [
        "docker",
        "run",
        "--name",
        worker_dir.name,
        "--memory",
        MEMORY_LIMIT,
        "--memory-swap",
        MEMORY_LIMIT,
        "--network",
        "host",
        "--user",
        "65532:65532",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--pids-limit",
        "128",
        "--tmpfs",
        "/tmp:rw,noexec,nosuid,size=64m",
        "--mount",
        f"type=bind,src={binary},dst=/worker/mariadb-mysql-cdc,readonly",
        "--mount",
        f"type=bind,src={worker_dir},dst=/worker/input,readonly",
        "--mount",
        "type=bind,src=/usr/lib,dst=/host-lib,readonly",
        "--mount",
        "type=bind,src=/usr/lib64,dst=/host-lib64,readonly",
        "--env",
        f"CDC_SOURCE_PASSWORD={SOURCE_PASSWORD}",
        "--env",
        f"CDC_TARGET_PASSWORD={TARGET_PASSWORD}",
        "--entrypoint",
        "/host-lib64/ld-linux-x86-64.so.2",
        WORKER_IMAGE,
        "--library-path",
        "/host-lib",
        "/worker/mariadb-mysql-cdc",
        "recover-lost-binlog",
        "--authorization-file",
        "/worker/input/authorization.json",
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
        "/worker/input/ca.pem",
    ]


def make_worker_input(
    evidence_dir: Path, name: str, authorization: Path, ca_file: Path
) -> Path:
    worker_dir = evidence_dir / name
    worker_dir.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(authorization, worker_dir / "authorization.json")
    shutil.copyfile(ca_file, worker_dir / "ca.pem")
    os.chmod(worker_dir, 0o755)
    for path in worker_dir.iterdir():
        os.chmod(path, 0o644)
    return worker_dir


def read_worker_result(name: str) -> WorkerResult:
    inspect_result = run(["docker", "inspect", name], check=False)
    if inspect_result.returncode:
        raise HarnessError(
            f"worker inspection failed name={name}: {inspect_result.stderr}"
        )
    inspect = json.loads(inspect_result.stdout)[0]
    state = inspect["State"]
    log_result = run(["docker", "logs", name], check=False)
    return WorkerResult(
        name=name,
        exit_code=int(state["ExitCode"]),
        oom_killed=bool(state["OOMKilled"]),
        error=str(state.get("Error", "")),
        log=f"{log_result.stdout}{log_result.stderr}",
        inspect=inspect,
    )


def run_worker(
    harness: Harness,
    binary: Path,
    authorization: Path,
    evidence_dir: Path,
    name: str,
) -> WorkerResult:
    worker_dir = make_worker_input(evidence_dir, name, authorization, harness.ca_file)
    command = worker_command(harness, binary, authorization, worker_dir)
    created = False
    try:
        started = run(command, check=False, timeout=900)
        created = True
        result = read_worker_result(name)
        result_path = evidence_dir / f"{name}.json"
        result_path.write_text(
            json.dumps(
                {
                    "command": command,
                    "docker_run_exit": started.returncode,
                    "worker": {
                        "exit_code": result.exit_code,
                        "oom_killed": result.oom_killed,
                        "error": result.error,
                        "inspect": result.inspect,
                    },
                    "log": result.log,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return result
    finally:
        if created:
            run(["docker", "rm", "-f", name], check=False)


def wait_for_prefix_progress(harness: Harness, recovery_id: str) -> str:
    assert harness.target
    deadline = time.monotonic() + 180
    last = ""
    while time.monotonic() < deadline:
        last = harness.admin_query(
            harness.target,
            "SELECT status,last_primary_key_json,chunks,rows_scanned "
            "FROM cdc.sync_runs WHERE "
            f"run_id={sql_literal(recovery_id)} AND stage='rows' AND table_name='memory_rows';",
        ).strip()
        fields = last.split("\t")
        if len(fields) == 4 and int(fields[2]) >= 6 and int(fields[3]) >= SMALL_ROWS:
            return last
        time.sleep(0.2)
    raise HarnessError(f"baseline did not persist six prefix chunks: {last!r}")


def assert_baseline_failure(
    harness: Harness,
    checkpoint: dict[str, object],
    recovery_id: str,
    result: WorkerResult,
) -> None:
    if result.exit_code != 137 or not result.oom_killed:
        raise HarnessError(
            "baseline was not Docker-cgroup OOMKilled exit 137: "
            f"exit={result.exit_code} oom_killed={result.oom_killed} error={result.error!r} log={result.log!r}"
        )
    if harness.checkpoint() != checkpoint:
        raise HarnessError("baseline OOM changed the old stream checkpoint")
    assert harness.target
    status = harness.admin_query(
        harness.target,
        "SELECT status FROM cdc.stream_recovery_records "
        f"WHERE recovery_id={sql_literal(recovery_id)};",
    ).strip()
    if status != "prepared":
        raise HarnessError(
            f"baseline OOM did not preserve prepared recovery: {status!r}"
        )
    progress = wait_for_prefix_progress(harness, recovery_id)
    print(f"baseline_oom_killed_137 prefix_progress={progress}")


def table_fingerprint(harness: Harness, endpoint: object) -> str:
    return harness.admin_query(
        endpoint,
        "SELECT COUNT(*),COALESCE(SUM(OCTET_LENGTH(body)),0),"
        "COALESCE(SUM(CRC32(CONCAT(id,'|',kind,'|',body))),0) FROM memory_rows;",
    ).strip()


def assert_candidate_success(
    harness: Harness,
    checkpoint: dict[str, object],
    barrier: dict[str, str],
    recovery_id: str,
    result: WorkerResult,
) -> None:
    if result.exit_code != 0 or result.oom_killed:
        raise HarnessError(
            "candidate did not complete below the identical 2 GiB cgroup cap: "
            f"exit={result.exit_code} oom_killed={result.oom_killed} error={result.error!r} log={result.log!r}"
        )
    assert harness.source and harness.target
    source = table_fingerprint(harness, harness.source)
    target = table_fingerprint(harness, harness.target)
    if target != source:
        raise HarnessError(
            f"candidate rows/bodies differ from source: source={source!r} target={target!r}"
        )
    rows = (
        harness.admin_query(
            harness.target,
            "SELECT id,body,kind FROM memory_rows WHERE id IN (1,60001,224764) ORDER BY id;",
        )
        .strip()
        .splitlines()
    )
    expected = [
        "1\tsmall-1\tsmall",
        f"60001\t{large_body_value(60001)}\tlarge",
        f"224764\t{large_body_value(224764)}\tlarge",
    ]
    if rows != expected:
        raise HarnessError(
            f"candidate did not apply source-authoritative updates: {rows!r}"
        )
    deleted = harness.admin_query(
        harness.target,
        "SELECT COUNT(*) FROM memory_rows WHERE id BETWEEN 61001 AND 215764;",
    ).strip()
    if deleted != "0":
        raise HarnessError(f"candidate retained target-only sparse-gap rows: {deleted}")
    original_recovery_id = lost.RECOVERY_ID
    try:
        lost.RECOVERY_ID = recovery_id
        lost.assert_committed_transition(harness, checkpoint, barrier)
    finally:
        lost.RECOVERY_ID = original_recovery_id
    advanced = harness.checkpoint()
    if advanced == checkpoint:
        raise HarnessError("candidate recovery did not advance the stream checkpoint")
    print(f"candidate_green_ok fingerprint={target} checkpoint={advanced}")


def large_body_value(identifier: int) -> str:
    prefix = f"large-{identifier}"
    return prefix + "x" * (LARGE_BODY_BYTES - len(prefix))


def run_case(
    binary: Path, evidence_dir: Path, label: str, keep: bool
) -> tuple[Harness, dict[str, object], dict[str, str], str, WorkerResult]:
    recovery_id = f"{RECOVERY_ID_PREFIX}-{label}"
    harness = Harness(REPO, binary, keep)
    harness.__enter__()
    try:
        harness.prepare()
        authorization, checkpoint, barrier = prepare_recovery(harness, recovery_id)
        result = run_worker(
            harness,
            binary,
            authorization,
            evidence_dir,
            f"{RECOVERY_ID_PREFIX}-{label}-{os.getpid()}",
        )
        return harness, checkpoint, barrier, recovery_id, result
    except Exception:
        harness.__exit__(*sys.exc_info())
        raise


def main() -> int:
    args = parse_args()
    evidence_dir = args.evidence_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    evidence_dir.mkdir(parents=True)
    baseline = verify_baseline(args.baseline)
    if args.candidate is None and not args.baseline_only:
        raise HarnessError("--candidate is required unless --baseline-only is selected")
    candidate = require_binary(args.candidate, "candidate") if args.candidate else None
    metadata = {
        "baseline": str(baseline),
        "baseline_sha256": sha256(baseline),
        "candidate": str(candidate) if candidate else None,
        "candidate_sha256": sha256(candidate) if candidate else None,
        "worker_image": WORKER_IMAGE,
        "docker_memory": MEMORY_LIMIT,
        "docker_memory_swap": MEMORY_LIMIT,
        "worker_uid": "65532:65532",
        "target_fixture": {
            "small_matched_prefix_rows": SMALL_ROWS,
            "large_target_rows": TARGET_LARGE_ROWS,
            "target_insert_batch_rows": TARGET_INSERT_BATCH_ROWS,
            "large_source_first_rows": SOURCE_LARGE_FIRST_ROWS,
            "large_source_last_rows": SOURCE_LARGE_LAST_ROWS,
            "large_body_bytes": LARGE_BODY_BYTES,
        },
    }
    (evidence_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True)
    )
    baseline_harness: Harness | None = None
    candidate_harness: Harness | None = None
    try:
        if not args.candidate_only:
            baseline_harness, checkpoint, _barrier, recovery_id, baseline_result = (
                run_case(baseline, evidence_dir, "baseline", args.keep)
            )
            assert_baseline_failure(
                baseline_harness, checkpoint, recovery_id, baseline_result
            )
            baseline_harness.__exit__(None, None, None)
            baseline_harness = None
            if args.baseline_only:
                print(f"sync_memory_baseline_red_ok evidence_dir={evidence_dir}")
                return 0

        assert candidate is not None
        candidate_harness, checkpoint, barrier, recovery_id, candidate_result = (
            run_case(candidate, evidence_dir, "candidate", args.keep)
        )
        assert_candidate_success(
            candidate_harness, checkpoint, barrier, recovery_id, candidate_result
        )
        candidate_harness.__exit__(None, None, None)
        candidate_harness = None
    except HarnessSkip as error:
        print(f"harness_skip prerequisite={error}")
        return 0
    except HarnessError as error:
        print(f"harness_error: {error}", file=sys.stderr)
        return 2
    finally:
        if candidate_harness is not None:
            candidate_harness.__exit__(None, None, None)
        if baseline_harness is not None:
            baseline_harness.__exit__(None, None, None)
    print(f"sync_memory_integration_harness_ok evidence_dir={evidence_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
