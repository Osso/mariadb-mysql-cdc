#!/usr/bin/env python3
"""Disposable process-kill/resume proof for an audited prepared recovery."""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BASE_PATH = REPO / "scripts/lost-binlog-integration-harness.py"
spec = importlib.util.spec_from_file_location("recovery_harness_base", BASE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError(f"cannot load {BASE_PATH}")
base = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = base
spec.loader.exec_module(base)

ROW_COUNT = 250_000


def seed_fixture(harness, rows: int, parallelism: int = 1) -> tuple[Path, dict]:
    schema = (
        "CREATE TABLE a_done (id BIGINT NOT NULL PRIMARY KEY, "
        "payload VARCHAR(64) NOT NULL, KEY idx_payload(payload)) ENGINE=InnoDB;"
        "CREATE TABLE b_rows (id BIGINT NOT NULL PRIMARY KEY, "
        "payload VARCHAR(2048) NOT NULL) ENGINE=InnoDB;"
    )
    harness.admin_sql(harness.source, schema)
    harness.admin_sql(harness.target, schema)
    harness.admin_sql(
        harness.source,
        "INSERT INTO a_done VALUES (1,'before-capture');"
        f"INSERT INTO b_rows SELECT seq, REPEAT('x',512) FROM seq_1_to_{rows};",
    )
    harness.admin_sql(
        harness.target,
        "INSERT INTO a_done VALUES (1,'target-divergent');"
        "GRANT LOCK TABLES ON globalcomix.* TO 'cdc_stream'@'%';"
        "GRANT CREATE ON cdc.* TO 'cdc_stream'@'%';"
        "GRANT SELECT,INSERT,UPDATE ON cdc.sync_runs TO 'cdc_stream'@'%';",
    )
    if parallelism == 2:
        for name in ("c_rows", "d_rows"):
            statement = f"CREATE TABLE {name} LIKE b_rows;"
            harness.admin_sql(harness.source, statement)
            harness.admin_sql(harness.target, statement)
            harness.admin_sql(
                harness.source, f"INSERT INTO {name} SELECT * FROM b_rows;"
            )
    start = harness.coordinate()
    harness.write_checkpoint(start)
    checkpoint = harness.checkpoint()
    harness.admin_sql(
        harness.source,
        "ALTER TABLE a_done RENAME INDEX idx_payload TO idx_payload_after;",
    )
    stream, _ = harness.start_stream(start, label="resume-fixture-barrier")
    try:
        barrier = base.wait_for_pending_barrier(harness, stream)
    finally:
        kill_process(stream)
    base.assert_checkpoint(harness, checkpoint)
    base.purge_checkpoint_history(harness, start)
    authorization = harness.tempdir / "resume-authorization.json"
    base.write_authorization(authorization, checkpoint, barrier, mismatch=False)
    return authorization, checkpoint


def kill_process(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.kill()
    process.wait(timeout=15)
    output = getattr(process, "_cdc_log", None)
    if output is not None:
        output.close()


def read_prepared(harness) -> dict:
    row = harness.admin_query(
        harness.target,
        "SELECT JSON_OBJECT('recovery_id',recovery_id,'status',status,"
        "'scope_hash',scope_hash,'new_checkpoint',CAST(new_checkpoint_json AS JSON),"
        "'prepared_at',CAST(prepared_at AS CHAR),'prepared_evidence',prepared_evidence_json) "
        f"FROM cdc.stream_recovery_records WHERE recovery_id={base.sql_literal(base.RECOVERY_ID)};",
    ).strip()
    return json.loads(row) if row else {}


def read_control_state(harness) -> tuple[str, str, dict]:
    records = harness.admin_query(
        harness.target,
        "SELECT recovery_id,status,old_checkpoint_json,new_checkpoint_json,"
        "prepared_evidence_json,CAST(prepared_at AS CHAR),committed_evidence_json,"
        "CAST(committed_at AS CHAR) FROM cdc.stream_recovery_records ORDER BY recovery_id;",
    )
    progress = harness.admin_query(
        harness.target,
        "SELECT run_id,stage,table_name,last_primary_key_json,chunks,rows_scanned,"
        "inserts_applied,updates_applied,deletes_applied,status,"
        "CAST(updated_at AS CHAR) FROM cdc.sync_runs ORDER BY run_id,stage,table_name;",
    )
    return records, progress, harness.checkpoint()


def read_completed_progress(harness) -> str:
    return harness.admin_query(
        harness.target,
        "SELECT stage,last_primary_key_json,chunks,rows_scanned,inserts_applied,"
        "updates_applied,deletes_applied,status,CAST(updated_at AS CHAR) "
        "FROM cdc.sync_runs WHERE table_name='a_done' "
        "AND stage IN ('prerequisite_schema','rows') ORDER BY stage;",
    )


def wait_for_partial_rows(harness, process, log: Path) -> None:
    deadline = time.monotonic() + 90
    while process.poll() is None and time.monotonic() < deadline:
        state = harness.admin_query(
            harness.target,
            "SELECT table_name,status,rows_scanned FROM cdc.sync_runs "
            "WHERE stage='rows' ORDER BY table_name;",
        ).splitlines()
        values = {
            fields[0]: fields[1:] for fields in (row.split("\t") for row in state)
        }
        done = values.get("a_done", [])
        partial = values.get("b_rows", [])
        if done and done[0] == "complete" and partial:
            scanned = int(partial[1])
            if partial[0] == "running" and 0 < scanned < ROW_COUNT:
                return
        time.sleep(0.05)
    raise base.HarnessError(
        f"did not observe complete plus partial rows: {log.read_text()}"
    )


def run_resume(harness, authorization: Path, parallelism: int = 1):
    args = base.recovery_args(harness, harness._sync_binary(), authorization)
    args[1] = "resume-lost-binlog"
    if parallelism != 1:
        args.extend(["--parallelism", str(parallelism)])
    return base.run(
        args, cwd=REPO, env=base.recovery_environment(), timeout=240, check=False
    )


def start_resume(harness, authorization: Path):
    args = base.recovery_args(harness, harness._sync_binary(), authorization)
    args[1] = "resume-lost-binlog"
    log = harness.tempdir / "prepared-resume.log"
    output = log.open("w")
    process = subprocess.Popen(
        args,
        cwd=REPO,
        env=base.recovery_environment(),
        stdout=output,
        stderr=subprocess.STDOUT,
        text=True,
    )
    setattr(process, "_cdc_log", output)
    return process, log


def assert_refused(harness, authorization: Path, markers: tuple[str, ...]) -> None:
    before = read_control_state(harness)
    result = run_resume(harness, authorization)
    error = result.stdout + result.stderr
    if result.returncode != 1 or not any(marker in error for marker in markers):
        raise base.HarnessError(
            f"unexpected resume refusal: {result.returncode}: {error}"
        )
    if read_control_state(harness) != before:
        raise base.HarnessError(
            "refused resume changed checkpoint, record, or durable progress"
        )


def assert_payload(harness, table: str, expected: str) -> None:
    actual = harness.admin_query(
        harness.target, f"SELECT payload FROM {table} WHERE id=1;"
    ).strip()
    if actual != expected:
        raise base.HarnessError(
            f"{table} prefix unexpectedly rescanned/replayed: {actual!r}"
        )


def wait_for_two_workers(harness, future) -> None:
    deadline = time.monotonic() + 60
    while not future.done() and time.monotonic() < deadline:
        counts = harness.admin_query(
            harness.target,
            "SELECT COUNT(DISTINCT l.OBJECT_NAME),COUNT(DISTINCT l.OWNER_THREAD_ID) "
            "FROM performance_schema.metadata_locks l "
            "JOIN performance_schema.threads t ON t.THREAD_ID=l.OWNER_THREAD_ID "
            "WHERE l.OBJECT_SCHEMA='globalcomix' AND l.OBJECT_NAME IN ('c_rows','d_rows') "
            "AND l.LOCK_STATUS='PENDING' AND l.LOCK_TYPE='SHARED_NO_READ_WRITE' "
            "AND t.PROCESSLIST_USER='cdc_stream';",
        ).strip()
        if counts == "2\t2":
            return
        time.sleep(0.05)
    if future.done():
        result = future.result()
        raise base.HarnessError(
            f"two workers not observed: {result.stdout}{result.stderr}"
        )
    raise base.HarnessError(
        "two independent table workers did not reach the database barrier"
    )


def resume_with_two_workers(harness, authorization: Path):
    blocker, owner = start_table_blocker(harness, ("c_rows", "d_rows"))
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run_resume, harness, authorization, 2)
        try:
            wait_for_two_workers(harness, future)
        finally:
            release_blocker(harness, blocker, owner)
        result = future.result(timeout=240)
    print("parallel_resume_overlap tables=2 independent_worker_connections=2")
    return result


def run_resume_case(binary: Path | None, keep: bool, parallelism: int) -> None:
    with base.Harness(REPO, binary, keep) as harness:
        harness.prepare()
        authorization, old_checkpoint = seed_fixture(harness, ROW_COUNT, parallelism)
        process, log = base.start_recovery(harness, authorization)
        try:
            wait_for_partial_rows(harness, process, log)
        finally:
            kill_process(process)
        prepared = read_prepared(harness)
        if prepared.get("status") != "prepared":
            raise base.HarnessError(f"interrupted record is not prepared: {prepared}")
        base.assert_checkpoint(harness, old_checkpoint)
        completed_progress = read_completed_progress(harness)

        bad = json.loads(authorization.read_text())
        bad["expected_checkpoint"]["source_position"] += 1
        mismatched = harness.tempdir / "mismatched-authorization.json"
        mismatched.write_text(json.dumps(bad))
        assert_refused(harness, mismatched, ("checkpoint", "authorized"))
        assert_held_stream_lease_refuses_resume(harness, authorization)

        harness.admin_sql(
            harness.source,
            "UPDATE a_done SET payload='after-capture' WHERE id=1;"
            "UPDATE b_rows SET payload='changed-prefix' WHERE id=1;"
            f"INSERT INTO b_rows VALUES ({ROW_COUNT + 1},'after-capture-tail');",
        )
        resumed = (
            resume_with_two_workers(harness, authorization)
            if parallelism == 2
            else run_resume(harness, authorization)
        )
        base.require_success(resumed, "prepared recovery resume")
        if parallelism == 2:
            for name in ("c_rows", "d_rows"):
                expected = f"{ROW_COUNT}\t{ROW_COUNT * 512}"
                actual = harness.admin_query(
                    harness.target,
                    f"SELECT COUNT(*),SUM(OCTET_LENGTH(payload)) FROM {name};",
                ).strip()
                if actual != expected:
                    raise base.HarnessError(
                        f"parallel table {name} did not converge: {actual}"
                    )
        report = json.loads(resumed.stdout)
        if report["new_checkpoint"] != prepared["new_checkpoint"]:
            raise base.HarnessError("resume replaced the original captured boundary")
        if read_completed_progress(harness) != completed_progress:
            raise base.HarnessError(
                "resume rewrote already-complete stage/table progress"
            )
        assert_payload(harness, "a_done", "before-capture")
        assert_payload(harness, "b_rows", "x" * 512)
        counts = harness.admin_query(
            harness.target, "SELECT COUNT(*),COUNT(DISTINCT id) FROM b_rows;"
        ).strip()
        if counts != f"{ROW_COUNT + 1}\t{ROW_COUNT + 1}":
            raise base.HarnessError(f"resumed rows are missing or duplicated: {counts}")
        committed = read_prepared(harness)
        if committed["status"] != "committed":
            raise base.HarnessError(f"resume did not commit: {committed}")
        for field in (
            "recovery_id",
            "scope_hash",
            "new_checkpoint",
            "prepared_at",
            "prepared_evidence",
        ):
            if committed[field] != prepared[field]:
                raise base.HarnessError(
                    f"resume mutated prepared identity/evidence field: {field}"
                )
        assert_refused(harness, authorization, ("not prepared", "checkpoint"))

        stop = harness.coordinate()
        original = prepared["new_checkpoint"]
        streamed = harness.run_stream(
            base.Coordinate(original["source_file"], original["source_position"]), stop
        )
        base.require_success(streamed, "replay from original resumed boundary")
        assert_payload(harness, "a_done", "after-capture")
        assert_payload(harness, "b_rows", "changed-prefix")
        actual = harness.checkpoint()
        if (actual["source_file"], actual["source_position"]) != (
            stop.file,
            stop.position,
        ):
            raise base.HarnessError(
                f"post-resume replay did not reach source stop: {actual}"
            )
        print(
            "prepared_resume_ok completed_table_preserved=true partial_cursor_preserved=true "
            "original_boundary_preserved=true post_capture_replay=true"
        )


def wait_for_fixture_table_lock(harness, blocker, tables: tuple[str, ...]) -> int:
    deadline = time.monotonic() + 15
    names = ",".join(base.sql_literal(name) for name in tables)
    while blocker.poll() is None and time.monotonic() < deadline:
        owner = harness.admin_query(
            harness.target,
            "SELECT t.PROCESSLIST_ID FROM performance_schema.metadata_locks l "
            "JOIN performance_schema.threads t ON t.THREAD_ID=l.OWNER_THREAD_ID "
            f"WHERE l.OBJECT_SCHEMA='globalcomix' AND l.OBJECT_NAME IN ({names}) "
            "AND l.LOCK_STATUS='GRANTED' AND t.PROCESSLIST_USER='root' "
            f"GROUP BY t.PROCESSLIST_ID HAVING COUNT(DISTINCT l.OBJECT_NAME)={len(tables)} LIMIT 1;",
        ).strip()
        if owner:
            return int(owner)
        time.sleep(0.05)
    raise base.HarnessError("fixture did not acquire the target table lock")


def start_table_blocker(harness, tables: tuple[str, ...] = ("b_rows",)):
    names = ",".join(f"globalcomix.{name} WRITE" for name in tables)
    process = harness.start_query(
        harness.target,
        f"LOCK TABLES {names}; DO SLEEP(120); UNLOCK TABLES;",
        user="root",
        password=base.h.ADMIN_PASSWORD,
    )
    return process, wait_for_fixture_table_lock(harness, process, tables)


def release_blocker(harness, process, connection_id: int) -> None:
    harness.admin_sql(harness.target, f"KILL CONNECTION {connection_id};")
    kill_process(process)


def assert_held_stream_lease_refuses_resume(harness, authorization: Path) -> None:
    lease = "SHA2('cdc-stream:globalcomix',256)"
    blocker = harness.start_query(
        harness.target,
        f"SELECT GET_LOCK({lease},0); DO SLEEP(90);",
        user="root",
        password=base.h.ADMIN_PASSWORD,
    )
    owner = None
    try:
        deadline = time.monotonic() + 15
        while blocker.poll() is None and time.monotonic() < deadline:
            value = harness.admin_query(
                harness.target, f"SELECT IS_USED_LOCK({lease});"
            ).strip()
            if value != "NULL":
                owner = int(value)
                break
            time.sleep(0.05)
        if owner is None:
            raise base.HarnessError("fixture failed to hold the live-stream lease")
        assert_refused(harness, authorization, ("lease",))
    finally:
        if owner is not None:
            release_blocker(harness, blocker, owner)
        else:
            kill_process(blocker)


def wait_for_resume_work(harness, process, log: Path) -> None:
    deadline = time.monotonic() + 30
    while process.poll() is None and time.monotonic() < deadline:
        count = harness.admin_query(
            harness.target,
            "SELECT COUNT(*) FROM performance_schema.metadata_locks l "
            "JOIN performance_schema.threads t ON t.THREAD_ID=l.OWNER_THREAD_ID "
            "WHERE l.OBJECT_SCHEMA='globalcomix' AND l.OBJECT_NAME='b_rows' "
            "AND l.LOCK_STATUS='PENDING' AND t.PROCESSLIST_USER='cdc_stream';",
        ).strip()
        if int(count) > 0:
            return
        time.sleep(0.05)
    raise base.HarnessError(
        f"resume did not reach blocked sync work: {log.read_text()}"
    )


def assert_boundary_expiry_before_commit(
    harness, authorization: Path, prepared: dict
) -> None:
    old_checkpoint = harness.checkpoint()
    blocker, owner = start_table_blocker(harness)
    process, log = start_resume(harness, authorization)
    try:
        wait_for_resume_work(harness, process, log)
        original = prepared["new_checkpoint"]
        base.purge_checkpoint_history(
            harness,
            base.Coordinate(original["source_file"], original["source_position"]),
        )
    except BaseException:
        kill_process(process)
        raise
    finally:
        release_blocker(harness, blocker, owner)
    try:
        result = base.finish_recovery(process, log)
    finally:
        kill_process(process)
    if result.returncode != 1 or "no longer retained" not in result.stdout:
        raise base.HarnessError(
            f"late binlog expiry did not refuse commit: {result.stdout}"
        )
    base.assert_checkpoint(harness, old_checkpoint)
    if read_prepared(harness) != prepared:
        raise base.HarnessError(
            "late binlog expiry changed the immutable prepared record"
        )
    finished = harness.admin_query(
        harness.target, "SELECT COUNT(*) FROM cdc.sync_runs WHERE status='complete';"
    ).strip()
    if finished != "6":
        raise base.HarnessError(
            f"late expiry was not exercised after full staged work: {finished}"
        )


def run_refusal_case(binary: Path | None, keep: bool) -> None:
    with base.Harness(REPO, binary, keep) as harness:
        harness.prepare()
        authorization, old_checkpoint = seed_fixture(harness, 100)
        blocker, owner = start_table_blocker(harness)
        process, _ = base.start_recovery(harness, authorization)
        try:
            base.wait_for_recovery_prepared(harness)
        finally:
            kill_process(process)
            release_blocker(harness, blocker, owner)
        base.assert_checkpoint(harness, old_checkpoint)
        prepared = read_prepared(harness)
        harness.admin_sql(
            harness.source,
            "CREATE TABLE changed_scope (id INT PRIMARY KEY) ENGINE=InnoDB;",
        )
        assert_refused(harness, authorization, ("scope changed",))
        harness.admin_sql(harness.source, "DROP TABLE changed_scope;")
        assert_boundary_expiry_before_commit(harness, authorization, prepared)
        assert_refused(harness, authorization, ("no longer retained",))
        print(
            "prepared_resume_refusals_ok changed_scope=true expired_before_work=true "
            "expired_before_commit=true checkpoint_unchanged=true"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--parallelism", type=int, choices=(1, 2), default=1)
    parser.add_argument("--case", choices=("resume", "refusals", "all"), default="all")
    args = parser.parse_args()
    try:
        if args.case in ("resume", "all"):
            run_resume_case(args.binary, args.keep, args.parallelism)
        if args.case in ("refusals", "all"):
            run_refusal_case(args.binary, args.keep)
    except base.HarnessError as error:
        print(f"harness_error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
