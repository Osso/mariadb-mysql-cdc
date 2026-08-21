#!/usr/bin/env python3
"""Disposable MariaDB -> MySQL CDC proof harness.

The executable scenarios use dedicated least-privilege accounts. Harness
administrative and target connections use TLS; the source stream intentionally
uses plaintext to match the accepted production source transport policy.
Scenarios without a production failpoint or real repair command are reported as
explicit prerequisites, never as passes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

APP_SCHEMA = "globalcomix"
CDC_SCHEMA = "cdc"
ADMIN_PASSWORD = "cdc-harness-password"
SOURCE_USER = "cdc_reader"
SOURCE_PASSWORD = "cdc-reader-password"
LIVE_TARGET_USER = "cdc_stream"
LIVE_TARGET_PASSWORD = "cdc-stream-password"
SYNC_TARGET_USER = "cdc_sync"
SYNC_TARGET_PASSWORD = "cdc-sync-password"
TARGET_USER = LIVE_TARGET_USER
TARGET_PASSWORD = LIVE_TARGET_PASSWORD
SOURCE_IMAGE = "mariadb:11.4"
TARGET_IMAGE = "mysql:8.0"
SOURCE_IDENTITY = "cdc-harness-source"


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    executable: bool
    prerequisite: str = ""


SCENARIOS = (
    ScenarioSpec("strict-secondary-btree", True),
    ScenarioSpec("sync-tls", True),
    ScenarioSpec("sync-composite-enum-primary-key", True),
    ScenarioSpec("sync-fk-parent-insert", True),
    ScenarioSpec("sync-fk-parent-update", True),
    ScenarioSpec("sync-fk-parent-stale-unique-owner", True),
    ScenarioSpec("sync-wide-update", True),
    ScenarioSpec("sync-resume", True),
    ScenarioSpec("sync-authorized-additive-spec-migration", True),
    ScenarioSpec("sync-progress-least-privilege", True),
    ScenarioSpec("writable-column-generated-metadata", True),
    ScenarioSpec("production-alter-table", True),
    ScenarioSpec("create-table-crash-restart", True),
    ScenarioSpec("bootstrap-contract", True),
    ScenarioSpec("insert-duplicate-idempotent", True),
    ScenarioSpec("missing-fk-parent-auto-insert", True),
    ScenarioSpec("missing-fk-nested-parent-auto-insert", True),
    ScenarioSpec("missing-fk-superseded-insert", True),
    ScenarioSpec("missing-fk-duplicate-parent-reconcile", True),
    ScenarioSpec("missing-checkpoint", True),
    ScenarioSpec("missing-trigger", True),
    ScenarioSpec("missing-grant", True),
    ScenarioSpec("journal-outage", True),
    ScenarioSpec("translation-pending-barrier", True),
    ScenarioSpec("prepare-failure", True),
    ScenarioSpec("post-ddl-pre-applied", True),
    ScenarioSpec("applied-pre-checkpoint", True),
    ScenarioSpec("checkpoint-transaction", True),
    ScenarioSpec("source-connection-loss", True),
    ScenarioSpec("target-connection-loss", True),
    ScenarioSpec("row-conflict-source-row-migration", True),
    ScenarioSpec("pre-state-drift", True),
    ScenarioSpec("coordinate-reuse", True),
    ScenarioSpec("raw-sql-reuse", True),
    ScenarioSpec("end-position-reuse", True),
    ScenarioSpec("checkpoint-mismatch", True),
)
SCENARIO_BY_NAME = {scenario.name: scenario for scenario in SCENARIOS}


class HarnessError(RuntimeError):
    pass


class HarnessSkip(RuntimeError):
    pass


@dataclass(frozen=True)
class Endpoint:
    container: str
    port: int


@dataclass(frozen=True)
class Coordinate:
    file: str
    position: int


@dataclass
class CommandResult:
    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


def default_scenarios() -> list[str]:
    return [scenario.name for scenario in SCENARIOS if scenario.executable]


class Harness:
    def __init__(self, repo: Path, binary: Path | None, keep: bool = False):
        self.repo = repo
        self.binary = binary
        self.keep = keep
        self.tempdir = Path(tempfile.mkdtemp(prefix="mariadb-mysql-cdc-harness-"))
        self.containers: list[str] = []
        self.source: Endpoint | None = None
        self.target: Endpoint | None = None
        self.ca_file = self.tempdir / "ca.pem"
        self.unrelated_ca_file = self.tempdir / "unrelated-ca.pem"
        self.cert_file = self.tempdir / "server-cert.pem"
        self.key_file = self.tempdir / "server-key.pem"

    def __enter__(self) -> "Harness":
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        if self.keep:
            print(f"harness_kept tempdir={self.tempdir}", file=sys.stderr)
            return
        for container in reversed(self.containers):
            run(["docker", "rm", "-f", container], check=False)
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def prepare(self) -> None:
        for command in ("docker", "mariadb", "openssl"):
            require_command(command)
        self._generate_tls_material()
        self.source = self._start_database("source", SOURCE_IMAGE, 101)
        self.target = self._start_database("target", TARGET_IMAGE, 102)
        wait_for_sql(self.source, self.ca_file)
        wait_for_sql(self.target, self.ca_file)
        self._bootstrap_endpoints()
        self._assert_endpoint_tls(self.source, SOURCE_USER, SOURCE_PASSWORD, "source")
        self._assert_endpoint_tls(
            self.target,
            LIVE_TARGET_USER,
            LIVE_TARGET_PASSWORD,
            "live target",
        )
        self._assert_endpoint_tls(
            self.target,
            SYNC_TARGET_USER,
            SYNC_TARGET_PASSWORD,
            "sync target",
        )
        self._assert_source_grants()
        self._assert_target_grants()

    def _generate_tls_material(self) -> None:
        ca_key = self.tempdir / "ca-key.pem"
        server_key = self.key_file
        csr = self.tempdir / "server.csr"
        extfile = self.tempdir / "server-ext.cnf"
        extfile.write_text(
            "subjectAltName=IP:127.0.0.1,DNS:localhost\n"
            "extendedKeyUsage=serverAuth\n"
        )
        run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-subj",
                "/CN=cdc-harness-ca",
                "-keyout",
                str(ca_key),
                "-out",
                str(self.ca_file),
            ]
        )
        run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-subj",
                "/CN=cdc-harness-unrelated-ca",
                "-keyout",
                str(self.tempdir / "unrelated-ca-key.pem"),
                "-out",
                str(self.unrelated_ca_file),
            ]
        )
        run(
            [
                "openssl",
                "req",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                "/CN=127.0.0.1",
                "-keyout",
                str(server_key),
                "-out",
                str(csr),
            ]
        )
        run(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                str(csr),
                "-CA",
                str(self.ca_file),
                "-CAkey",
                str(ca_key),
                "-CAcreateserial",
                "-days",
                "2",
                "-extfile",
                str(extfile),
                "-out",
                str(self.cert_file),
            ]
        )
        make_tls_material_container_readable(
            self.tempdir,
            [self.ca_file, self.cert_file, self.key_file],
        )

    def _start_database(self, role: str, image: str, server_id: int) -> Endpoint:
        name = f"mariadb-mysql-cdc-harness-{role}-{os.getpid()}"
        self.containers.append(name)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        args = [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "-e",
            f"MYSQL_ROOT_PASSWORD={ADMIN_PASSWORD}",
            "-e",
            f"MARIADB_ROOT_PASSWORD={ADMIN_PASSWORD}",
            "-e",
            f"MYSQL_DATABASE={APP_SCHEMA}",
            "-e",
            f"MARIADB_DATABASE={APP_SCHEMA}",
            "-v",
            f"{self.tempdir}:/etc/cdc-tls:ro",
            "-p",
            f"127.0.0.1:{port}:3306",
            image,
            "--server-id=" + str(server_id),
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--binlog-row-metadata=FULL",
            "--ssl-ca=/etc/cdc-tls/ca.pem",
            "--ssl-cert=/etc/cdc-tls/server-cert.pem",
            "--ssl-key=/etc/cdc-tls/server-key.pem",
        ]
        run(args)
        return Endpoint(name, port)

    def _bootstrap_endpoints(self) -> None:
        assert self.source and self.target
        self.admin_sql_file(self.source, self.repo / "fixtures/cdc-harness-source-bootstrap.sql")
        self.admin_sql_file(self.target, self.repo / "fixtures/cdc-harness-target-bootstrap.sql")

    def _assert_endpoint_tls(self, endpoint: Endpoint, user: str, password: str, label: str) -> None:
        values = self.query(endpoint, "SHOW STATUS LIKE 'Ssl_cipher';", user=user, password=password)
        rows = [line.split("\t", 1) for line in values.splitlines() if "\t" in line]
        cipher = next((value for name, value in rows if name == "Ssl_cipher"), "")
        if not cipher:
            raise HarnessError(f"{label} TLS identity/cipher validation was not observable: {values!r}")
        print(f"endpoint_tls_diagnostics label={label} port={endpoint.port} cipher={cipher}")

    def _assert_source_grants(self) -> None:
        assert self.source
        grants = self.admin_query(self.source, "SHOW GRANTS FOR 'cdc_reader'@'%';")
        normalized = normalize_grants(grants)
        assert_exact_grants(
            normalized,
            {
                (frozenset({"USAGE"}), "*.*"),
                (frozenset({"REPLICATION SLAVE", "REPLICATION CLIENT"}), "*.*"),
                (frozenset({"SELECT", "SHOW VIEW"}), f"{APP_SCHEMA}.*"),
            },
            SOURCE_USER,
        )

    def _assert_target_grants(self) -> None:
        assert self.target
        application_grant = (
            frozenset(
                {
                    "SELECT",
                    "INSERT",
                    "UPDATE",
                    "DELETE",
                    "CREATE",
                    "ALTER",
                    "DROP",
                    "INDEX",
                    "REFERENCES",
                    "CREATE VIEW",
                    "SHOW VIEW",
                    "CREATE ROUTINE",
                    "ALTER ROUTINE",
                    "EXECUTE",
                    "EVENT",
                    "TRIGGER",
                }
            ),
            f"{APP_SCHEMA}.*",
        )
        live_grants = {
            (frozenset({"USAGE"}), "*.*"),
            application_grant,
            (frozenset({"SELECT", "INSERT", "UPDATE"}), "cdc.stream_checkpoint"),
            (frozenset({"SELECT", "INSERT", "UPDATE"}), "cdc.ddl_replay_journal"),
            (frozenset({"EXECUTE"}), "PROCEDURE cdc.ddl_replay_journal_trigger_inventory"),
        }
        sync_application_grant = (
            application_grant[0].union({"LOCK TABLES"}),
            application_grant[1],
        )
        sync_grants = {
            (frozenset({"USAGE"}), "*.*"),
            sync_application_grant,
            (frozenset({"CREATE"}), "cdc.*"),
            (frozenset({"SELECT", "INSERT", "UPDATE"}), "cdc.stream_checkpoint"),
            (frozenset({"SELECT", "INSERT", "UPDATE"}), "cdc.sync_runs"),
        }
        for user, expected in (
            (LIVE_TARGET_USER, live_grants),
            (SYNC_TARGET_USER, sync_grants),
        ):
            grants = self.admin_query(self.target, f"SHOW GRANTS FOR '{user}'@'%';")
            print(f"{user}_show_grants_begin")
            for row in grants.splitlines():
                print(f"{user}_show_grant row={row}")
            print(f"{user}_show_grants_end")
            assert_exact_grants(normalize_grants(grants), expected, user)

    def refresh_endpoint(self, endpoint: Endpoint) -> Endpoint:
        port_text = run(["docker", "port", endpoint.container, "3306/tcp"]).stdout.strip()
        try:
            return Endpoint(endpoint.container, int(port_text.rsplit(":", 1)[1]))
        except (IndexError, ValueError) as error:
            raise HarnessError(f"could not refresh Docker port for {endpoint.container}: {port_text!r}") from error

    def admin_sql_file(self, endpoint: Endpoint, path: Path) -> str:
        if not path.is_file():
            raise HarnessError(f"bootstrap fixture missing: {path}")
        return self._mysql(endpoint, path.read_text(), "root", ADMIN_PASSWORD)

    def admin_sql(self, endpoint: Endpoint, sql: str) -> str:
        return self._mysql(endpoint, sql, "root", ADMIN_PASSWORD)

    def admin_query(self, endpoint: Endpoint, sql: str) -> str:
        return self.admin_sql(endpoint, sql)

    def assert_admin_sql_rejected(self, endpoint: Endpoint, sql: str, expected_error: str) -> None:
        try:
            self.admin_sql(endpoint, sql)
        except HarnessError as error:
            if expected_error.lower() not in str(error).lower():
                raise HarnessError(
                    f"SQL failed for the wrong reason endpoint={endpoint.container}: {error}"
                ) from error
            return
        raise HarnessError(f"SQL unexpectedly succeeded endpoint={endpoint.container}: {sql}")

    def query(self, endpoint: Endpoint, sql: str, *, user: str, password: str) -> str:
        return self._mysql(endpoint, sql, user, password)

    def wait_for_data_lock_wait(
        self,
        endpoint: Endpoint,
        process: subprocess.Popen[str],
        query_marker: str,
        timeout: float = 30,
    ) -> str:
        evidence_sql = (
            "SELECT waiting.PROCESSLIST_INFO, waiting.PROCESSLIST_STATE, "
            "waits.REQUESTING_THREAD_ID, waits.BLOCKING_THREAD_ID "
            "FROM performance_schema.data_lock_waits waits "
            "JOIN performance_schema.threads waiting "
            "ON waiting.THREAD_ID=waits.REQUESTING_THREAD_ID "
            "WHERE waiting.PROCESSLIST_USER='cdc_stream' "
            f"AND waiting.PROCESSLIST_INFO LIKE {sql_literal('%' + query_marker + '%')} "
            "LIMIT 1;"
        )
        deadline = time.monotonic() + timeout
        while True:
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise HarnessError(
                    "blocked INSERT exited before MySQL exposed its lock wait: "
                    f"exit={process.returncode} stdout={stdout!r} stderr={stderr!r}"
                )
            evidence = self.admin_query(endpoint, evidence_sql).strip()
            if evidence:
                fields = evidence.split("\t")
                if len(fields) != 4 or "INSERT INTO" not in fields[0]:
                    raise HarnessError(f"unexpected INSERT lock-wait evidence: {evidence!r}")
                print(f"failed_run_claim_second_connection_blocked evidence={evidence!r}")
                return evidence
            if time.monotonic() >= deadline:
                raise HarnessError(
                    "MySQL never exposed the second connection's INSERT in data_lock_waits: "
                    f"marker={query_marker!r}"
                )
            time.sleep(0.05)

    def start_query(
        self,
        endpoint: Endpoint,
        sql: str,
        *,
        user: str,
        password: str,
    ) -> subprocess.Popen[str]:
        process = subprocess.Popen(
            [
                "mariadb",
                "--protocol=tcp",
                "--ssl",
                f"--ssl-ca={self.ca_file}",
                "--ssl-verify-server-cert",
                "--host=127.0.0.1",
                f"--port={endpoint.port}",
                f"--user={user}",
                f"--password={password}",
                f"--database={APP_SCHEMA}",
                "--batch",
                "--raw",
                "--skip-column-names",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert process.stdin is not None
        process.stdin.write(sql)
        process.stdin.close()
        return process

    def _mysql(self, endpoint: Endpoint, sql: str, user: str, password: str) -> str:
        result = run(
            [
                "mariadb",
                "--protocol=tcp",
                "--ssl",
                f"--ssl-ca={self.ca_file}",
                "--ssl-verify-server-cert",
                "--host=127.0.0.1",
                f"--port={endpoint.port}",
                f"--user={user}",
                f"--password={password}",
                f"--database={APP_SCHEMA}",
                "--batch",
                "--raw",
                "--skip-column-names",
            ],
            input_text=sql,
            check=False,
        )
        if result.returncode:
            raise HarnessError(
                f"SQL failed endpoint={endpoint.container} port={endpoint.port} user={user}:\n"
                f"{result.stderr.strip()}\nSQL:\n{sql}"
            )
        return result.stdout

    def coordinate(self) -> Coordinate:
        assert self.source
        row = self.query(self.source, "SHOW MASTER STATUS;", user=SOURCE_USER, password=SOURCE_PASSWORD).splitlines()[0].split("\t")
        return Coordinate(row[0], int(row[1]))

    def write_checkpoint(self, coordinate: Coordinate) -> None:
        assert self.target
        checkpoint = {
            "source_file": coordinate.file,
            "source_position": coordinate.position,
            "gtid": None,
            "event_timestamp": 0,
            "last_event": {"event_type": "bootstrap", "description": "harness bootstrap"},
        }
        name = f"stream-binlog:{SOURCE_IDENTITY}"
        sql = (
            "INSERT INTO cdc.stream_checkpoint (checkpoint_name, checkpoint_json) VALUES ("
            f"{sql_literal(name)}, {sql_literal(json.dumps(checkpoint, separators=(',', ':')))}"
            ") ON DUPLICATE KEY UPDATE checkpoint_json=VALUES(checkpoint_json);"
        )
        self.query(self.target, sql, user=TARGET_USER, password=TARGET_PASSWORD)

    def checkpoint(self) -> dict:
        assert self.target
        name = f"stream-binlog:{SOURCE_IDENTITY}"
        value = self.query(
            self.target,
            f"SELECT checkpoint_json FROM cdc.stream_checkpoint WHERE checkpoint_name={sql_literal(name)};",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if not value:
            raise HarnessError("stream checkpoint missing")
        return json.loads(value)

    def _stream_binary(self, integration_failpoint: str | None) -> Path:
        binary = self.binary or self.repo / "target/debug/mariadb-mysql-cdc"
        if integration_failpoint is not None:
            binary = self.repo / "target/debug/mariadb-mysql-cdc"
        build = ["cargo", "build"]
        if integration_failpoint is not None:
            build.extend(["--features", "integration-failpoints"])
        build.extend(["--bin", "mariadb-mysql-cdc"])
        source_based_binary = self.binary is None
        if source_based_binary or integration_failpoint is not None:
            run(build, cwd=self.repo)
        if not binary.is_file():
            raise HarnessError(f"CDC binary build did not produce {binary}")
        return binary

    def _stream_args(
        self,
        binary: Path,
        start: Coordinate,
        stop: Coordinate | None,
        integration_failpoint: str | None,
        max_reconnects: int,
    ) -> list[str]:
        assert self.source and self.target
        args = [
            str(binary),
            "stream-binlog",
            "--source-host",
            "127.0.0.1",
            "--source-port",
            str(self.source.port),
            "--source-user",
            SOURCE_USER,
            "--source-password-env",
            "CDC_SOURCE_PASSWORD",
            "--source-database",
            APP_SCHEMA,
            "--source-identity",
            SOURCE_IDENTITY,
            "--binlog-file",
            start.file,
            "--start-position",
            str(start.position),
            "--target-host",
            "127.0.0.1",
            "--target-port",
            str(self.target.port),
            "--target-user",
            LIVE_TARGET_USER,
            "--target-password-env",
            "CDC_TARGET_PASSWORD",
            "--target-database",
            APP_SCHEMA,
            "--target-tls-ca-file",
            str(self.ca_file),
            "--max-reconnects",
            str(max_reconnects),
        ]
        if stop:
            args.extend(["--stop-position", str(stop.position)])
        if integration_failpoint is not None:
            args.extend(["--integration-failpoint", integration_failpoint])
        return args

    def run_stream(
        self,
        start: Coordinate,
        stop: Coordinate | None = None,
        integration_failpoint: str | None = None,
        max_reconnects: int = 0,
        barrier_dir: Path | None = None,
    ) -> CommandResult:
        binary = self._stream_binary(integration_failpoint)
        env = {
            **os.environ,
            "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD,
            "CDC_TARGET_PASSWORD": LIVE_TARGET_PASSWORD,
        }
        if barrier_dir is not None:
            env["CDC_INTEGRATION_BARRIER_DIR"] = str(barrier_dir)
        return run(
            self._stream_args(
                binary,
                start,
                stop,
                integration_failpoint,
                max_reconnects,
            ),
            env=env,
            timeout=90,
            check=False,
        )

    def _sync_binary(self) -> Path:
        binary = self.binary or self.repo / "target/debug/mariadb-mysql-cdc"
        if self.binary is None:
            run(["cargo", "build", "--bin", "mariadb-mysql-cdc"], cwd=self.repo)
        if not binary.is_file():
            raise HarnessError(f"CDC binary build did not produce {binary}")
        return binary

    def _sync_args(
        self,
        binary: Path,
        *,
        tables: list[str],
        run_id: str,
        chunk_size: int = 1000,
        parallelism: int = 1,
        progress_table: str = "cdc.sync_runs",
        target_ca_file: Path | None = None,
        authorized_old_run_spec_sha256: str | None = None,
    ) -> list[str]:
        assert self.source and self.target
        args = [
            str(binary),
            "sync",
            "--source-host",
            "127.0.0.1",
            "--source-port",
            str(self.source.port),
            "--source-user",
            SOURCE_USER,
            "--source-password-env",
            "CDC_SOURCE_PASSWORD",
            "--source-database",
            APP_SCHEMA,
            "--target-host",
            "127.0.0.1",
            "--target-port",
            str(self.target.port),
            "--target-user",
            SYNC_TARGET_USER,
            "--target-password-env",
            "CDC_TARGET_PASSWORD",
            "--target-database",
            APP_SCHEMA,
            "--target-tls-ca-file",
            str(target_ca_file or self.ca_file),
            "--chunk-size",
            str(chunk_size),
            "--parallelism",
            str(parallelism),
            "--progress-table",
            progress_table,
            "--run-id",
            run_id,
        ]
        if authorized_old_run_spec_sha256 is not None:
            args.extend(
                [
                    "--authorize-old-run-spec-sha256",
                    authorized_old_run_spec_sha256,
                ]
            )
        for table in tables:
            args.extend(["--table", table])
        return args

    def run_sync(
        self,
        *,
        tables: list[str],
        run_id: str,
        chunk_size: int = 1000,
        parallelism: int = 1,
        progress_table: str = "cdc.sync_runs",
        timeout: float = 180,
        authorized_old_run_spec_sha256: str | None = None,
    ) -> CommandResult:
        binary = self._sync_binary()
        env = {
            **os.environ,
            "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD,
            "CDC_TARGET_PASSWORD": SYNC_TARGET_PASSWORD,
        }
        return run(
            self._sync_args(
                binary,
                tables=tables,
                run_id=run_id,
                chunk_size=chunk_size,
                parallelism=parallelism,
                progress_table=progress_table,
                authorized_old_run_spec_sha256=authorized_old_run_spec_sha256,
            ),
            cwd=self.repo,
            env=env,
            timeout=timeout,
            check=False,
        )

    def start_sync(
        self,
        *,
        tables: list[str],
        run_id: str,
        chunk_size: int,
        parallelism: int = 1,
    ) -> tuple[subprocess.Popen[str], Path]:
        binary = self._sync_binary()
        log_path = self.tempdir / f"{run_id}.log"
        log = log_path.open("w")
        process = subprocess.Popen(
            self._sync_args(
                binary,
                tables=tables,
                run_id=run_id,
                chunk_size=chunk_size,
                parallelism=parallelism,
            ),
            cwd=self.repo,
            env={
                **os.environ,
                "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD,
                "CDC_TARGET_PASSWORD": SYNC_TARGET_PASSWORD,
            },
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        process._cdc_log = log  # type: ignore[attr-defined]
        return process, log_path

    def start_stream(
        self,
        start: Coordinate,
        stop: Coordinate | None = None,
        integration_failpoint: str | None = None,
        max_reconnects: int = 0,
        barrier_dir: Path | None = None,
        label: str = "stream",
    ) -> tuple[subprocess.Popen[str], Path]:
        binary = self._stream_binary(integration_failpoint)
        env = {
            **os.environ,
            "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD,
            "CDC_TARGET_PASSWORD": LIVE_TARGET_PASSWORD,
        }
        if barrier_dir is not None:
            env["CDC_INTEGRATION_BARRIER_DIR"] = str(barrier_dir)
        log_path = self.tempdir / f"{label}.log"
        log = log_path.open("w")
        process = subprocess.Popen(
            self._stream_args(
                binary,
                start,
                stop,
                integration_failpoint,
                max_reconnects,
            ),
            cwd=self.repo,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        process._cdc_log = log  # type: ignore[attr-defined]
        return process, log_path

    def wait_for_barrier(
        self,
        process: subprocess.Popen[str],
        barrier_dir: Path,
        boundary: str,
        timeout: float = 60.0,
    ) -> None:
        ready = barrier_dir / f"{boundary}.ready"
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if ready.is_file():
                return
            if process.poll() is not None:
                raise HarnessError(f"stream exited before barrier {boundary}: {self.process_output(process)}")
            time.sleep(0.1)
        raise HarnessError(f"stream did not reach barrier {boundary}: {self.process_output(process)}")

    def release_barrier(self, barrier_dir: Path, boundary: str) -> None:
        (barrier_dir / f"{boundary}.release").write_text("release")

    def process_output(self, process: subprocess.Popen[str]) -> str:
        log = getattr(process, "_cdc_log", None)
        if log is not None:
            log.flush()
        path = self.tempdir / "missing.log"
        if log is not None:
            path = Path(log.name)
        return path.read_text() if path.is_file() else ""

    def finish_stream(self, process: subprocess.Popen[str]) -> CommandResult:
        process.wait(timeout=90)
        output = self.process_output(process)
        log = getattr(process, "_cdc_log", None)
        if log is not None:
            log.close()
        return CommandResult(("stream-binlog",), process.returncode or 0, output, "")

    def setup_accounts_table(self) -> None:
        assert self.source and self.target
        schema = """
            CREATE TABLE accounts (
                id BIGINT NOT NULL PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                payload VARCHAR(64) NOT NULL,
                KEY idx_accounts_payload (payload)
            ) ENGINE=InnoDB;
        """
        self.admin_sql(self.source, schema)
        self.admin_sql(self.target, schema)

    def _assert_sync_target_unchanged(self) -> None:
        assert self.target
        row_count = self.admin_query(self.target, "SELECT COUNT(*) FROM accounts;").strip()
        progress_count = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM cdc.sync_runs;",
        ).strip()
        if row_count != "0" or progress_count != "0":
            raise HarnessError(
                "rejected sync mutated target: "
                f"rows={row_count!r} progress_rows={progress_count!r}"
            )

    def _assert_sync_ca_rejected(
        self,
        binary: Path,
        env: dict[str, str],
        *,
        target_ca_file: Path,
        label: str,
    ) -> None:
        result = run(
            self._sync_args(
                binary,
                tables=["accounts"],
                run_id=f"sync-tls-rejected-{label.replace(' ', '-')}",
                chunk_size=2,
                parallelism=2,
                target_ca_file=target_ca_file,
            ),
            env=env,
            timeout=90,
            check=False,
        )
        if result.returncode == 0:
            raise HarnessError(f"sync accepted {label}")
        diagnostic = " ".join((result.stdout, result.stderr)).lower()
        if not any(marker in diagnostic for marker in ("certificate", "ssl", "tls")):
            raise HarnessError(f"sync {label} lacked TLS diagnostic: {diagnostic!r}")
        self._assert_sync_target_unchanged()

    def run_sync_tls(self) -> None:
        assert self.source and self.target
        self.setup_accounts_table()
        self.admin_sql(
            self.source,
            "INSERT INTO accounts VALUES "
            "(1, 'one@example.test', 'one'),"
            "(2, 'two@example.test', 'two'),"
            "(3, 'three@example.test', 'three'),"
            "(4, 'four@example.test', 'four');",
        )
        binary = self._sync_binary()
        args = self._sync_args(
            binary,
            tables=["accounts"],
            run_id="sync-tls",
            chunk_size=2,
            parallelism=2,
        )
        env = {
            **os.environ,
            "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD,
            "CDC_TARGET_PASSWORD": SYNC_TARGET_PASSWORD,
        }

        self._assert_sync_ca_rejected(
            binary,
            env,
            target_ca_file=self.unrelated_ca_file,
            label="untrusted target CA",
        )

        first = run(args, cwd=self.repo, env=env, timeout=90, check=False)
        require_success(first, "unified sync TLS")
        expected_rows = [
            "1	one@example.test	one",
            "2	two@example.test	two",
            "3	three@example.test	three",
            "4	four@example.test	four",
        ]
        copied_rows = self.query(
            self.target,
            "SELECT id,email,payload FROM accounts ORDER BY id;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).splitlines()
        if copied_rows != expected_rows:
            raise HarnessError(f"sync TLS copied rows mismatch: {copied_rows!r}")
        progress_rows = self.admin_query(
            self.target,
            "SELECT stage,table_name,status,rows_scanned FROM cdc.sync_runs "
            "WHERE run_id='sync-tls' ORDER BY FIELD(stage,'prerequisite_schema','rows','final_constraints');",
        ).splitlines()
        expected_progress = [
            "prerequisite_schema	accounts	complete	0",
            "rows	accounts	complete	4",
            "final_constraints	accounts	complete	0",
        ]
        if progress_rows != expected_progress:
            raise HarnessError(f"sync TLS progress mismatch: {progress_rows!r}")

        self.admin_sql(
            self.target,
            "SET GLOBAL general_log=OFF; TRUNCATE TABLE mysql.general_log; "
            "SET GLOBAL log_output='TABLE'; SET GLOBAL general_log=ON;",
        )
        replay = run(args, cwd=self.repo, env=env, timeout=90, check=False)
        mutation_attempts = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM mysql.general_log WHERE user_host LIKE 'cdc_sync%' "
            "AND command_type IN ('Query','Prepare','Execute') "
            "AND (UPPER(argument) LIKE 'INSERT%ACCOUNTS%' "
            "OR UPPER(argument) LIKE 'UPDATE%ACCOUNTS%' "
            "OR UPPER(argument) LIKE 'DELETE%ACCOUNTS%');",
        ).strip()
        self.admin_sql(self.target, "SET GLOBAL general_log=OFF;")
        require_success(replay, "completed unified sync rerun")
        if mutation_attempts != "0":
            raise HarnessError(
                "completed sync rerun attempted account mutations: "
                f"{mutation_attempts}"
            )
        print(
            "sync_tls_converged rows=4 target_ca=true wrong_target_ca_rejected=true "
            "parallelism=2 completed_rerun_noop=true"
        )

    def run_insert_duplicate_idempotent(self) -> None:
        assert self.source and self.target
        self.setup_accounts_table()
        self.admin_sql(
            self.target,
            "INSERT INTO accounts VALUES (1, 'target@example.test', 'target-only'); "
            "DROP PROCEDURE cdc.row_conflicts_trigger_inventory; "
            "DROP TABLE cdc.row_conflicts;",
        )
        start = self.coordinate()
        self.write_checkpoint(start)
        self.admin_sql(
            self.source,
            "START TRANSACTION; "
            "INSERT INTO accounts VALUES (1, 'source@example.test', 'source'); "
            "INSERT INTO accounts VALUES (2, 'two@example.test', 'two'); "
            "COMMIT;",
        )
        stop = self.coordinate()

        result = self.run_stream(start, stop)
        require_success(result, "serial source-authoritative duplicate INSERT stream")
        rows = self.query(
            self.target,
            "SELECT id,email,payload FROM accounts ORDER BY id;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        expected = "1\ttarget@example.test\ttarget-only\n2\ttwo@example.test\ttwo"
        if rows != expected:
            raise HarnessError(
                f"serial duplicate INSERT changed target authority or skipped later row: {rows!r}"
            )
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != stop.file or int(
            checkpoint.get("source_position", 0)
        ) != stop.position:
            raise HarnessError(
                f"serial duplicate INSERT checkpoint did not reach exact stop: {checkpoint}"
            )
        conflict_tables = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='cdc' AND table_name='row_conflicts';",
        ).strip()
        if conflict_tables != "0":
            raise HarnessError(
                f"serial duplicate INSERT recreated retired live conflict table: {conflict_tables}"
            )
        print(
            "insert_duplicate_idempotent_ok mode=serial target_row=unchanged "
            "later_same_transaction_row=applied conflict_table=absent "
            f"checkpoint={stop.file}:{stop.position}"
        )

    def run_missing_fk_parent_auto_insert(self) -> None:
        assert self.source and self.target
        schema = """
            CREATE TABLE guests (
                guest_id BIGINT NOT NULL,
                guest_hash CHAR(32) NOT NULL,
                label VARCHAR(64) NOT NULL,
                PRIMARY KEY (guest_id, guest_hash)
            ) ENGINE=InnoDB;
            CREATE TABLE sessions (
                session_id BIGINT NOT NULL PRIMARY KEY,
                guest_id BIGINT NOT NULL,
                guest_hash CHAR(32) NOT NULL,
                payload VARCHAR(64) NOT NULL,
                CONSTRAINT sessions_fk_sessions_guest
                    FOREIGN KEY (guest_id, guest_hash)
                    REFERENCES guests (guest_id, guest_hash)
                    ON DELETE RESTRICT ON UPDATE RESTRICT
            ) ENGINE=InnoDB;
        """
        self.admin_sql(self.source, schema)
        self.admin_sql(self.target, schema)
        self.admin_sql(
            self.source,
            "INSERT INTO guests VALUES (41, '0123456789abcdef0123456789abcdef', 'source-parent');",
        )
        start = self.coordinate()
        self.write_checkpoint(start)
        self.admin_sql(
            self.source,
            "INSERT INTO sessions VALUES "
            "(7001, 41, '0123456789abcdef0123456789abcdef', 'child');",
        )
        stop = self.coordinate()

        result = self.run_stream(start, stop)
        require_success(result, "missing FK parent auto-insert stream")
        parent = self.query(
            self.target,
            "SELECT guest_id,guest_hash,label FROM guests;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        child = self.query(
            self.target,
            "SELECT session_id,guest_id,guest_hash,payload FROM sessions;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if parent != "41\t0123456789abcdef0123456789abcdef\tsource-parent":
            raise HarnessError(f"missing FK parent was not copied from source: {parent!r}")
        if child != "7001\t41\t0123456789abcdef0123456789abcdef\tchild":
            raise HarnessError(f"child row was not retried after parent copy: {child!r}")
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != stop.file or int(
            checkpoint.get("source_position", 0)
        ) != stop.position:
            raise HarnessError(
                f"missing FK repair checkpoint did not reach exact stop: {checkpoint}"
            )
        print(
            "missing_fk_parent_auto_insert_ok parent=guests child=sessions "
            f"checkpoint={stop.file}:{stop.position}"
        )

    def run_missing_fk_superseded_insert(self) -> None:
        assert self.source and self.target
        schema = """
            CREATE TABLE comics (
                id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                comic_format_id TINYINT UNSIGNED NOT NULL,
                label VARCHAR(64) NOT NULL,
                UNIQUE KEY uq_comics_id_format (id, comic_format_id)
            ) ENGINE=InnoDB;
            CREATE TABLE releases (
                id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                comic_id BIGINT UNSIGNED NOT NULL,
                comic_format_id TINYINT UNSIGNED NOT NULL,
                payload VARCHAR(64) NOT NULL,
                CONSTRAINT releases_ibfk_format
                    FOREIGN KEY (comic_id, comic_format_id)
                    REFERENCES comics (id, comic_format_id)
                    ON DELETE RESTRICT ON UPDATE CASCADE
            ) ENGINE=InnoDB;
        """
        self.admin_sql(self.source, schema)
        self.admin_sql(self.target, schema)
        self.admin_sql(
            self.source,
            "INSERT INTO comics VALUES (49868, 2, 'source-parent');",
        )
        start = self.coordinate()
        self.admin_sql(
            self.source,
            "INSERT INTO releases VALUES (391468, 49868, 2, 'historical'); "
            "UPDATE comics SET comic_format_id = 1, label = 'source-current-parent' "
            "WHERE id = 49868; "
            "UPDATE releases SET payload = 'source-current-child' WHERE id = 391468;",
        )
        stop = self.coordinate()

        self.admin_sql(
            self.target,
            "DELETE FROM releases; DELETE FROM comics; "
            "INSERT INTO comics VALUES (49868, 1, 'source-current-parent');",
        )
        self.write_checkpoint(start)
        result = self.run_stream(start, stop)
        require_success(result, "serial superseded missing-FK insert stream")
        child = self.query(
            self.target,
            "SELECT id,comic_id,comic_format_id,payload FROM releases;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        expected = "391468\t49868\t1\tsource-current-child"
        if child != expected:
            raise HarnessError(
                f"serial superseded source INSERT did not converge: {child!r}"
            )
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != stop.file or int(
            checkpoint.get("source_position", 0)
        ) != stop.position:
            raise HarnessError(
                f"serial superseded INSERT checkpoint did not reach exact stop: {checkpoint}"
            )
        output = f"{result.stdout}\n{result.stderr}"
        if "cdc_missing_fk_superseded_insert_reconciled" not in output:
            raise HarnessError(
                f"serial superseded INSERT did not report reconciliation: {output}"
            )
        print(
            "missing_fk_superseded_insert_ok "
            f"mode=serial current_source_row=applied checkpoint={stop.file}:{stop.position}"
        )

    def setup_missing_fk_duplicate_parent_tables(self) -> None:
        assert self.source and self.target
        schema = """
            CREATE TABLE users (
                id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                name VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                label VARCHAR(64) NOT NULL,
                UNIQUE KEY uq_users_id_name (id, name)
            ) ENGINE=InnoDB;
            CREATE TABLE artists_favorites (
                id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                user_id BIGINT UNSIGNED NOT NULL,
                user_name VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                CONSTRAINT artists_favorites_ibfk_2
                    FOREIGN KEY (user_id, user_name) REFERENCES users (id, name)
                    ON DELETE RESTRICT ON UPDATE RESTRICT
            ) ENGINE=InnoDB;
            CREATE TABLE comics (
                id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                slug VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                label VARCHAR(64) NOT NULL,
                UNIQUE KEY slug (slug)
            ) ENGINE=InnoDB;
            CREATE TABLE releases (
                id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                comic_id BIGINT UNSIGNED NOT NULL,
                CONSTRAINT releases_ibfk_6
                    FOREIGN KEY (comic_id) REFERENCES comics (id)
                    ON DELETE RESTRICT ON UPDATE RESTRICT
            ) ENGINE=InnoDB;
        """
        self.admin_sql(self.source, schema)
        self.admin_sql(self.target, schema)
        self.admin_sql(
            self.source,
            "INSERT INTO users VALUES (2108466, 'OvalTeen', 'source-user'); "
            "INSERT INTO comics VALUES "
            "(44083, 'old-night-shift', 'source-owner'), "
            "(49125, 'night-shift', 'source-parent'), "
            "(49126, 'deleted-owner', 'source-deleted-owner-parent');",
        )

    def seed_missing_fk_duplicate_parent_target(self) -> None:
        assert self.target
        self.admin_sql(
            self.target,
            "DELETE FROM artists_favorites; "
            "DELETE FROM releases; "
            "DELETE FROM users; "
            "DELETE FROM comics; "
            "INSERT INTO users VALUES (2108466, 'Oval-Teen', 'target-divergent'); "
            "INSERT INTO comics VALUES "
            "(44083, 'night-shift', 'target-stale-owner'), "
            "(44084, 'deleted-owner', 'target-source-absent-owner');",
        )

    def assert_missing_fk_duplicate_parent_result(
        self,
        mode: str,
        stop: Coordinate,
    ) -> None:
        assert self.target
        users = self.query(
            self.target,
            "SELECT id,name,label FROM users ORDER BY id;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        favorites = self.query(
            self.target,
            "SELECT id,user_id,user_name FROM artists_favorites ORDER BY id;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        comics = self.query(
            self.target,
            "SELECT id,slug,label FROM comics ORDER BY id;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        releases = self.query(
            self.target,
            "SELECT id,comic_id FROM releases ORDER BY id;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if users != "2108466\tOvalTeen\tsource-user":
            raise HarnessError(f"{mode} same-PK parent did not converge: {users!r}")
        if favorites != "1\t2108466\tOvalTeen":
            raise HarnessError(f"{mode} same-PK child was not retried: {favorites!r}")
        expected_comics = (
            "44083\told-night-shift\tsource-owner\n"
            "49125\tnight-shift\tsource-parent\n"
            "49126\tdeleted-owner\tsource-deleted-owner-parent"
        )
        if comics != expected_comics:
            raise HarnessError(f"{mode} unique owners did not converge: {comics!r}")
        if releases != "391409\t49125\n391410\t49126":
            raise HarnessError(f"{mode} comic children were not retried: {releases!r}")
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != stop.file or int(
            checkpoint.get("source_position", 0)
        ) != stop.position:
            raise HarnessError(
                f"{mode} duplicate-parent checkpoint did not reach exact stop: {checkpoint}"
            )

    def run_missing_fk_duplicate_parent_reconcile(self) -> None:
        assert self.source and self.target
        self.setup_missing_fk_duplicate_parent_tables()
        start = self.coordinate()
        self.admin_sql(
            self.source,
            "START TRANSACTION; "
            "INSERT INTO artists_favorites VALUES (1, 2108466, 'OvalTeen'); "
            "COMMIT; "
            "START TRANSACTION; "
            "INSERT INTO releases VALUES (391409, 49125), (391410, 49126); "
            "COMMIT;",
        )
        stop = self.coordinate()

        self.seed_missing_fk_duplicate_parent_target()
        self.write_checkpoint(start)
        result = self.run_stream(start, stop)
        require_success(result, "serial duplicate-parent reconciliation stream")
        self.assert_missing_fk_duplicate_parent_result("serial", stop)
        print(
            "missing_fk_duplicate_parent_reconcile_ok "
            "mode=serial same_pk=updated different_pk=updated "
            f"source_absent=deleted checkpoint={stop.file}:{stop.position}"
        )

    def run_missing_fk_nested_parent_auto_insert(self) -> None:
        assert self.source and self.target
        schema = """
            CREATE TABLE utms (
                id BIGINT NOT NULL PRIMARY KEY,
                label VARCHAR(64) NOT NULL
            ) ENGINE=InnoDB;
            CREATE TABLE guests (
                id BIGINT NOT NULL PRIMARY KEY,
                utm_id BIGINT NOT NULL,
                label VARCHAR(64) NOT NULL,
                CONSTRAINT guests_fk_guests_utm_id
                    FOREIGN KEY (utm_id) REFERENCES utms (id)
                    ON DELETE RESTRICT ON UPDATE RESTRICT
            ) ENGINE=InnoDB;
            CREATE TABLE sessions (
                id BIGINT NOT NULL PRIMARY KEY,
                guest_id BIGINT NOT NULL,
                payload VARCHAR(64) NOT NULL,
                CONSTRAINT sessions_fk_sessions_guest
                    FOREIGN KEY (guest_id) REFERENCES guests (id)
                    ON DELETE RESTRICT ON UPDATE RESTRICT
            ) ENGINE=InnoDB;
        """
        self.admin_sql(self.source, schema)
        self.admin_sql(self.target, schema)
        self.admin_sql(
            self.source,
            "INSERT INTO utms VALUES (501, 'source-utm'); "
            "INSERT INTO guests VALUES (41, 501, 'source-guest');",
        )
        start = self.coordinate()
        self.write_checkpoint(start)
        self.admin_sql(
            self.source,
            "INSERT INTO sessions VALUES (7001, 41, 'serial-child');",
        )
        stop = self.coordinate()

        result = self.run_stream(start, stop)
        require_success(result, "serial nested missing-FK parent stream")
        parents = self.query(
            self.target,
            "SELECT u.id,u.label,g.id,g.utm_id,g.label "
            "FROM utms u JOIN guests g ON g.utm_id=u.id;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        child = self.query(
            self.target,
            "SELECT id,guest_id,payload FROM sessions;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if parents != "501\tsource-utm\t41\t501\tsource-guest":
            raise HarnessError(
                f"serial target did not recursively copy missing parents: {parents!r}"
            )
        if child != "7001\t41\tserial-child":
            raise HarnessError(
                f"serial target did not retry child after nested parent repair: {child!r}"
            )
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != stop.file or int(
            checkpoint.get("source_position", 0)
        ) != stop.position:
            raise HarnessError(
                f"serial nested missing-FK checkpoint did not reach exact stop: {checkpoint}"
            )
        print(
            "missing_fk_nested_parent_auto_insert_ok mode=serial "
            "parents=recursive child=retried "
            f"checkpoint={stop.file}:{stop.position}"
        )

    def run_strict_secondary_btree(self) -> None:
        assert self.source and self.target
        self.setup_accounts_table()
        start = self.coordinate()
        self.write_checkpoint(start)
        self.admin_sql(
            self.source,
            "CREATE INDEX idx_accounts_email ON accounts (email);",
        )
        create_stop = self.coordinate()
        first = self.run_stream(start, create_stop)
        require_success(first, "strict-secondary-btree CREATE INDEX")
        created = self.query(
            self.target,
            "SELECT index_name, non_unique, seq_in_index, column_name, collation, index_type "
            "FROM information_schema.statistics "
            "WHERE table_schema='globalcomix' AND table_name='accounts' "
            "AND index_name='idx_accounts_email';",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        )
        if created.strip() != "idx_accounts_email\t1\t1\temail\tA\tBTREE":
            raise HarnessError(f"target missing complete replayed BTREE index metadata:\n{created}")

        self.admin_sql(
            self.source,
            "INSERT INTO accounts VALUES (1, 'one@example.test', 'one'); DROP INDEX idx_accounts_email ON accounts;",
        )
        stop = self.coordinate()
        second = self.run_stream(create_stop, stop)
        require_success(second, "strict-secondary-btree DROP INDEX and row replay")
        final_indexes = self.query(
            self.target,
            "SHOW INDEX FROM accounts;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        )
        if "idx_accounts_email" in final_indexes:
            raise HarnessError(f"target retained dropped index:\n{final_indexes}")
        count = self.query(
            self.target,
            "SELECT COUNT(*) FROM accounts;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if count != "1":
            raise HarnessError(f"target row count mismatch after DDL/DML replay: {count!r}")
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != stop.file or int(checkpoint.get("source_position", 0)) != stop.position:
            raise HarnessError(f"checkpoint did not reach exact source end coordinate {stop}: {checkpoint}")
        journal = self.query(
            self.target,
            "SELECT source_identity,status,binlog_file,event_start_position,event_end_position "
            "FROM cdc.ddl_replay_journal "
            "WHERE source_identity LIKE 'cdc-harness-source#server-id=%' "
            "ORDER BY event_start_position;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        )
        rows = [line.split("\t") for line in journal.splitlines() if line.strip()]
        if len(rows) != 2 or any(row[1] != "checkpointed" for row in rows):
            raise HarnessError(f"DDL journal did not contain two checkpointed rows:\n{journal}")
        pending = self.query(
            self.target,
            "SELECT COUNT(*) FROM cdc.ddl_replay_journal WHERE status IN ('translation_pending','blocked');",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if pending != "0":
            raise HarnessError(f"unexpected unresolved DDL journal debt after strict replay: {pending}")
        print(f"strict_secondary_btree_ok coordinate={stop.file}:{stop.position} journal_rows={len(rows)}")

    def run_production_alter_table(self) -> None:
        assert self.source and self.target
        schema = """
            CREATE TABLE home_feed_panel_candidates (
                id BIGINT NOT NULL PRIMARY KEY,
                filter_reason VARCHAR(64) DEFAULT NULL
            ) ENGINE=InnoDB;
            CREATE TABLE home_feed_bakes (
                id BIGINT NOT NULL PRIMARY KEY,
                reading_direction TINYINT UNSIGNED NOT NULL,
                status TINYINT UNSIGNED NOT NULL,
                published_time DATETIME DEFAULT NULL
            ) ENGINE=InnoDB;
            CREATE TABLE accounts (
                id BIGINT NOT NULL PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                handle VARCHAR(64) DEFAULT NULL
            ) ENGINE=InnoDB;
            INSERT INTO accounts VALUES (1, 'existing@example.test', 'existing');
        """
        self.admin_sql(self.source, schema)
        self.admin_sql(self.target, schema)
        start = self.coordinate()
        self.write_checkpoint(start)
        self.admin_sql(
            self.source,
            """
            ALTER TABLE home_feed_panel_candidates
              ADD COLUMN filter_prompt_version VARCHAR(64) DEFAULT NULL COMMENT 'sanitized description' AFTER filter_reason,
              ADD COLUMN filtered_time DATETIME NULL DEFAULT NULL COMMENT 'sanitized description' AFTER filter_prompt_version;
            ALTER TABLE home_feed_bakes
              ADD COLUMN variant_id SMALLINT UNSIGNED DEFAULT NULL AFTER reading_direction,
              ADD KEY idx_hfb_variant_status_published (variant_id, status, published_time);
            ALTER TABLE accounts ADD UNIQUE KEY uq_accounts_email (email);
            ALTER TABLE accounts DROP COLUMN IF EXISTS handle;
            """,
        )
        stop = self.coordinate()
        result = self.run_stream(start, stop)
        require_success(result, "production ALTER TABLE replay")
        columns = self.query(
            self.target,
            "SELECT table_name,column_name,column_type,is_nullable,column_default,column_comment "
            "FROM information_schema.columns WHERE table_schema='globalcomix' "
            "AND ((table_name='home_feed_panel_candidates' AND column_name IN ('filter_prompt_version','filtered_time')) "
            "OR (table_name='home_feed_bakes' AND column_name='variant_id')) "
            "ORDER BY table_name,ordinal_position;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).splitlines()
        expected_columns = [
            "home_feed_bakes\tvariant_id\tsmallint unsigned\tYES\tNULL\t",
            "home_feed_panel_candidates\tfilter_prompt_version\tvarchar(64)\tYES\tNULL\tsanitized description",
            "home_feed_panel_candidates\tfiltered_time\tdatetime\tYES\tNULL\tsanitized description",
        ]
        if columns != expected_columns:
            raise HarnessError(f"production ALTER TABLE column parity failed: {columns}")
        index_rows = self.query(
            self.target,
            "SELECT index_name,non_unique,seq_in_index,column_name,index_type "
            "FROM information_schema.statistics WHERE table_schema='globalcomix' "
            "AND table_name='home_feed_bakes' AND index_name='idx_hfb_variant_status_published' "
            "ORDER BY seq_in_index;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).splitlines()
        if index_rows != [
            "idx_hfb_variant_status_published\t1\t1\tvariant_id\tBTREE",
            "idx_hfb_variant_status_published\t1\t2\tstatus\tBTREE",
            "idx_hfb_variant_status_published\t1\t3\tpublished_time\tBTREE",
        ]:
            raise HarnessError(f"production ALTER TABLE index parity failed: {index_rows}")
        unique_metadata = []
        for endpoint in (self.source, self.target):
            unique_metadata.append(
                self.admin_query(
                    endpoint,
                    "SELECT index_name,non_unique,seq_in_index,column_name,sub_part,index_type "
                    "FROM information_schema.statistics WHERE table_schema='globalcomix' "
                    "AND table_name='accounts' AND index_name='uq_accounts_email' "
                    "ORDER BY seq_in_index;",
                ).strip()
            )
        expected_unique_metadata = "uq_accounts_email\t0\t1\temail\tNULL\tBTREE"
        if unique_metadata != [expected_unique_metadata, expected_unique_metadata]:
            raise HarnessError(f"production ADD UNIQUE KEY metadata parity failed: {unique_metadata}")
        duplicate_sql = "INSERT INTO accounts (id,email) VALUES (2, 'existing@example.test');"
        for endpoint in (self.source, self.target):
            self.assert_admin_sql_rejected(endpoint, duplicate_sql, "Duplicate entry")
            rows = self.admin_query(endpoint, "SELECT id,email FROM accounts ORDER BY id;").strip()
            if rows != "1\texisting@example.test":
                raise HarnessError(
                    f"production ADD UNIQUE KEY duplicate rejection mutated rows "
                    f"endpoint={endpoint.container}: {rows!r}"
                )
        journal = self.query(
            self.target,
            "SELECT status,transformation_version,CHAR_LENGTH(canonical_ast)>0,"
            "CHAR_LENGTH(pre_state)>0,CHAR_LENGTH(expected_post_state)>0 "
            "FROM cdc.ddl_replay_journal ORDER BY event_start_position;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).splitlines()
        expected_journal_row = "checkpointed\tmariadb-mysql8-v1\t1\t1\t1"
        if journal != [expected_journal_row] * 4:
            raise HarnessError(f"production ALTER TABLE journal mismatch: {journal}")
        unique_evidence_row = self.query(
            self.target,
            "SELECT status,transformation_version,generated_sql,canonical_ast,pre_state,expected_post_state "
            "FROM cdc.ddl_replay_journal "
            "WHERE raw_sql LIKE 'ALTER TABLE accounts ADD UNIQUE KEY%';",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        unique_evidence_fields = unique_evidence_row.split("\t", 5)
        if len(unique_evidence_fields) != 6:
            raise HarnessError(f"production ADD UNIQUE KEY evidence shape mismatch: {unique_evidence_row!r}")
        status, version, generated_sql, ast_json, pre_state_json, post_state_json = unique_evidence_fields
        expected_generated_sql = "ALTER TABLE `accounts` ADD UNIQUE KEY `uq_accounts_email` (`email`)"
        if (status, version, generated_sql) != (
            "checkpointed",
            "mariadb-mysql8-v1",
            expected_generated_sql,
        ):
            raise HarnessError(
                "production ADD UNIQUE KEY journal identity mismatch: "
                f"{(status, version, generated_sql)!r}"
            )
        expected_index_ast = {
            "create": True,
            "name": "uq_accounts_email",
            "table": "accounts",
            "unique": True,
            "index_type": "BTREE",
            "visible": True,
            "comment": None,
            "key_parts": [
                {
                    "column": "email",
                    "prefix_length": None,
                    "order": "ASC",
                    "collation": "A",
                }
            ],
        }
        expected_ast = {
            "family": "table",
            "object_kind": "table",
            "primary_object": "accounts",
            "secondary_object": None,
            "parsed_index": None,
            "parsed_alter_table": {
                "table": "accounts",
                "clauses": [{"kind": "add_key", "index": expected_index_ast}],
            },
            "parsed_create_table": None,
        }
        canonical_ast = json.loads(ast_json)
        if canonical_ast != expected_ast:
            raise HarnessError(f"production ADD UNIQUE KEY canonical AST mismatch: {canonical_ast!r}")
        pre_state = json.loads(pre_state_json)
        post_state = json.loads(post_state_json)
        expected_state_keys = {"kind", "name", "definition", "indexes", "foreign_keys"}
        if set(pre_state) != expected_state_keys or set(post_state) != expected_state_keys:
            raise HarnessError(
                f"production ADD UNIQUE KEY state shape mismatch: pre={pre_state!r} post={post_state!r}"
            )
        if pre_state["definition"] != post_state["definition"] or pre_state["foreign_keys"] != post_state["foreign_keys"]:
            raise HarnessError(
                f"production ADD UNIQUE KEY changed unrelated state: pre={pre_state!r} post={post_state!r}"
            )
        expected_index_state = {
            "table": "accounts",
            "name": "uq_accounts_email",
            "unique": True,
            "index_type": "BTREE",
            "visible": True,
            "comment": None,
            "columns": [
                {
                    "name": "email",
                    "sequence": 1,
                    "prefix_length": None,
                    "collation": "A",
                    "order": "ASC",
                }
            ],
        }
        if pre_state["indexes"] != [] or post_state["indexes"] != [expected_index_state]:
            raise HarnessError(
                f"production ADD UNIQUE KEY post-state mismatch: pre={pre_state!r} post={post_state!r}"
            )
        dropped_columns = []
        for endpoint in (self.source, self.target):
            dropped_columns.append(
                self.admin_query(
                    endpoint,
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema='globalcomix' AND table_name='accounts' "
                    "AND column_name='handle';",
                ).strip()
            )
        if dropped_columns != ["0", "0"]:
            raise HarnessError(f"DROP COLUMN IF EXISTS parity failed: {dropped_columns}")
        drop_evidence = self.query(
            self.target,
            "SELECT status,transformation_version,generated_sql "
            "FROM cdc.ddl_replay_journal WHERE raw_sql LIKE '%DROP COLUMN IF EXISTS handle%';",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if drop_evidence != (
            "checkpointed\tmariadb-mysql8-v1\t"
            "ALTER TABLE `accounts` DROP COLUMN `handle`"
        ):
            raise HarnessError(f"DROP COLUMN IF EXISTS evidence mismatch: {drop_evidence!r}")
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != stop.file or int(checkpoint.get("source_position", 0)) != stop.position:
            raise HarnessError(f"production ALTER TABLE checkpoint mismatch: {checkpoint}")
        supported_checkpoint = checkpoint

        self.admin_sql(self.source, "ALTER TABLE accounts DROP COLUMN IF EXISTS handle;")
        no_op_stop = self.coordinate()
        no_op_result = self.run_stream(stop, no_op_stop)
        require_success(no_op_result, "DROP COLUMN IF EXISTS proven no-op replay")
        no_op_evidence = self.query(
            self.target,
            "SELECT status,transformation_version,generated_sql "
            "FROM cdc.ddl_replay_journal WHERE raw_sql LIKE '%DROP COLUMN IF EXISTS handle%' "
            "ORDER BY event_start_position DESC LIMIT 1;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if no_op_evidence != "checkpointed\tmariadb-mysql8-v1\tNULL":
            raise HarnessError(f"DROP COLUMN IF EXISTS no-op evidence mismatch: {no_op_evidence!r}")
        no_op_checkpoint = self.checkpoint()
        if no_op_checkpoint.get("source_file") != no_op_stop.file or int(
            no_op_checkpoint.get("source_position", 0)
        ) != no_op_stop.position:
            raise HarnessError(f"DROP COLUMN IF EXISTS no-op checkpoint mismatch: {no_op_checkpoint}")
        supported_checkpoint = no_op_checkpoint

        self.admin_sql(
            self.target,
            "SET GLOBAL general_log=OFF; TRUNCATE TABLE mysql.general_log; "
            "SET GLOBAL log_output='TABLE'; SET GLOBAL general_log=ON;",
        )
        self.admin_sql(
            self.source,
            "ALTER TABLE accounts ADD UNIQUE KEY uq_accounts_email_prefix (email(8));",
        )
        pending_stop = self.coordinate()
        pending_result = self.run_stream(no_op_stop, pending_stop)
        require_translation_pending_termination(pending_result)
        pending_index = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM information_schema.statistics "
            "WHERE table_schema='globalcomix' AND table_name='accounts' "
            "AND index_name='uq_accounts_email_prefix';",
        ).strip()
        if pending_index != "0":
            raise HarnessError("unsupported unique-key option mutated target schema")
        target_execution_attempts = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM mysql.general_log WHERE user_host LIKE 'cdc_stream%' "
            "AND command_type IN ('Query','Prepare','Execute') "
            "AND argument LIKE 'ALTER TABLE%uq_accounts_email_prefix%';",
        ).strip()
        self.admin_sql(self.target, "SET GLOBAL general_log=OFF;")
        if target_execution_attempts != "0":
            raise HarnessError(
                "unsupported unique-key option reached target execution: "
                f"attempts={target_execution_attempts}"
            )
        pending_rows = self.query(
            self.target,
            "SELECT status,transformation_version,generated_sql,canonical_ast,pre_state,expected_post_state "
            "FROM cdc.ddl_replay_journal WHERE raw_sql LIKE '%uq_accounts_email_prefix%';",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).splitlines()
        if pending_rows != ["translation_pending\ttranslator-unavailable\tNULL\t\t\t"]:
            raise HarnessError(f"unsupported unique-key journal evidence mismatch: {pending_rows}")
        pending_checkpoint = self.checkpoint()
        if pending_checkpoint != supported_checkpoint:
            raise HarnessError(
                "unsupported unique-key option changed checkpoint: "
                f"before={supported_checkpoint} after={pending_checkpoint}"
            )
        print(
            f"production_alter_table_ok coordinate={no_op_stop.file}:{no_op_stop.position} "
            "journal_rows=5 unique_parity=true drop_column=true drop_noop=true "
            "pending_unique_option=true"
        )

    def run_create_table_crash_restart(self) -> None:
        assert self.source and self.target
        self.admin_sql(
            self.source,
            f"ALTER DATABASE {APP_SCHEMA} CHARACTER SET latin1 COLLATE latin1_swedish_ci;",
        )
        self.admin_sql(
            self.target,
            f"ALTER DATABASE {APP_SCHEMA} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;",
        )
        source_default = self.admin_query(
            self.source,
            f"SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME={sql_literal(APP_SCHEMA)};",
        ).strip()
        target_default = self.admin_query(
            self.target,
            f"SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME={sql_literal(APP_SCHEMA)};",
        ).strip()
        if source_default != "latin1_swedish_ci" or target_default != "utf8mb4_0900_ai_ci":
            raise HarnessError(
                f"CREATE TABLE defaults were not intentionally different source={source_default!r} target={target_default!r}"
            )

        self.admin_sql(self.target, "SET GLOBAL log_output='TABLE'; SET GLOBAL general_log=ON;")
        start = self.coordinate()
        self.write_checkpoint(start)
        self.admin_sql(
            self.source,
            "CREATE TABLE accounts ("
            "id BIGINT NOT NULL PRIMARY KEY, "
            "email VARCHAR(255) NOT NULL, "
            "payload VARCHAR(64) NOT NULL, "
            "KEY idx_accounts_payload (payload)"
            ") ENGINE=InnoDB;",
        )
        final_stop = self.coordinate()

        crashed = self.run_stream(
            start,
            final_stop,
            integration_failpoint="post-ddl-pre-applied",
        )
        crash_output = f"{crashed.stdout}\n{crashed.stderr}"
        if crashed.returncode == 0 or "cdc_integration_failpoint" not in crash_output:
            raise HarnessError(
                f"CREATE TABLE stream did not crash after target execution: {crash_output}"
            )
        checkpoint_after_crash = self.checkpoint()
        if (
            checkpoint_after_crash.get("source_file") != start.file
            or int(checkpoint_after_crash.get("source_position", 0)) != start.position
        ):
            raise HarnessError(
                f"CREATE TABLE crash advanced checkpoint: {checkpoint_after_crash}"
            )
        table_count = self.admin_query(
            self.target,
            f"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA={sql_literal(APP_SCHEMA)} AND TABLE_NAME='accounts';",
        ).strip()
        if table_count != "1":
            raise HarnessError(f"CREATE TABLE crash produced target table count={table_count}")
        target_collation = self.admin_query(
            self.target,
            f"SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA={sql_literal(APP_SCHEMA)} AND TABLE_NAME='accounts';",
        ).strip()
        if target_collation != source_default:
            raise HarnessError(
                f"CREATE TABLE did not preserve source collation source={source_default} target={target_collation}"
            )
        evidence = self.admin_query(
            self.target,
            "SELECT status,generated_sql,canonical_ast,pre_state,expected_post_state "
            "FROM cdc.ddl_replay_journal "
            "WHERE source_identity LIKE 'cdc-harness-source#server-id=%' "
            "ORDER BY event_start_position;",
        )
        evidence_rows = [line.split("\t") for line in evidence.splitlines() if line.strip()]
        if len(evidence_rows) != 1 or len(evidence_rows[0]) != 5:
            raise HarnessError(f"CREATE TABLE durable evidence row mismatch: {evidence!r}")
        status, generated_sql, canonical_ast, pre_state, expected_post_state = evidence_rows[0]
        if status != "prepared":
            raise HarnessError(f"CREATE TABLE crash journal status={status!r}")
        if "DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci" not in generated_sql:
            raise HarnessError(f"CREATE TABLE generated SQL omitted source defaults: {generated_sql}")
        if '"character_set":"latin1"' not in canonical_ast or '"collation":"latin1_swedish_ci"' not in canonical_ast:
            raise HarnessError(f"CREATE TABLE canonical evidence omitted source defaults: {canonical_ast}")
        if not pre_state or not expected_post_state or '"collation":"latin1_swedish_ci"' not in expected_post_state:
            raise HarnessError("CREATE TABLE durable pre/post evidence is incomplete")

        def target_create_count() -> str:
            return self.admin_query(
                self.target,
                "SELECT COUNT(*) FROM mysql.general_log "
                "WHERE command_type IN ('Query','Execute') "
                "AND argument LIKE 'CREATE TABLE `accounts`%';",
            ).strip()

        if target_create_count() != "1":
            raise HarnessError("CREATE TABLE target execution count was not exactly one after crash")

        restarted = self.run_stream(start, final_stop)
        require_success(restarted, "CREATE TABLE prepared-state restart")
        if "cdc_ddl_reconcile_prepared" not in restarted.stdout:
            raise HarnessError("CREATE TABLE restart did not reconcile prepared state")
        checkpoint_after_restart = self.checkpoint()
        if (
            checkpoint_after_restart.get("source_file") != final_stop.file
            or int(checkpoint_after_restart.get("source_position", 0)) != final_stop.position
        ):
            raise HarnessError(
                f"CREATE TABLE restart did not advance checkpoint exactly to event end: {checkpoint_after_restart}"
            )
        if target_create_count() != "1":
            raise HarnessError("CREATE TABLE restart re-executed target DDL")
        replayed = self.run_stream(start, final_stop)
        require_success(replayed, "CREATE TABLE idempotent replay")
        if self.checkpoint() != checkpoint_after_restart:
            raise HarnessError("CREATE TABLE idempotent replay changed checkpoint state")
        if target_create_count() != "1":
            raise HarnessError("CREATE TABLE idempotent replay executed target DDL again")
        final_status = self.admin_query(
            self.target,
            "SELECT status FROM cdc.ddl_replay_journal "
            "WHERE source_identity LIKE 'cdc-harness-source#server-id=%';",
        ).strip()
        if final_status != "checkpointed":
            raise HarnessError(f"CREATE TABLE final journal status={final_status!r}")
        print(
            "create_table_crash_restart_converged "
            f"source_default={source_default} target_default={target_default} "
            f"target_collation={target_collation} target_create_count=1 "
            f"checkpoint={final_stop.file}:{final_stop.position}"
        )

    def ddl_journal_rows(self) -> list[list[str]]:
        assert self.target
        output = self.query(
            self.target,
            "SELECT status,CHAR_LENGTH(canonical_ast),CHAR_LENGTH(pre_state),CHAR_LENGTH(expected_post_state),raw_sql "
            "FROM cdc.ddl_replay_journal "
            "WHERE source_identity LIKE 'cdc-harness-source#server-id=%' "
            "ORDER BY event_start_position;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        )
        return [line.split("\t") for line in output.splitlines() if line.strip()]

    def assert_recovery_state(
        self,
        coordinate: Coordinate,
        *,
        expected_status: str,
        expected_index: bool,
        expected_rows: str,
    ) -> None:
        assert self.target
        rows = self.ddl_journal_rows()
        if len(rows) != 1 or rows[0][0] != expected_status:
            raise HarnessError(f"unexpected DDL journal recovery state: {rows}")
        if any(int(value) <= 0 for value in rows[0][1:4]):
            raise HarnessError(f"DDL journal missing persisted evidence: {rows}")
        indexes = self.query(
            self.target,
            "SHOW INDEX FROM accounts;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        )
        has_index = "idx_accounts_email" in indexes
        if has_index != expected_index:
            raise HarnessError(f"target schema state mismatch expected_index={expected_index}: {indexes}")
        count = self.query(
            self.target,
            "SELECT COUNT(*) FROM accounts;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if count != expected_rows:
            raise HarnessError(f"later DML overtook DDL boundary: expected rows={expected_rows}, got {count}")
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != coordinate.file or int(checkpoint.get("source_position", 0)) != coordinate.position:
            raise HarnessError(f"checkpoint mismatch expected {coordinate.file}:{coordinate.position}: {checkpoint}")

    def journal_full_row(self) -> dict[str, str]:
        assert self.target
        output = self.query(
            self.target,
            "SELECT source_identity,source_server_id,binlog_file,event_start_position,"
            "event_end_position,schema_name,raw_sql,transformation_version,generated_sql,canonical_ast,"
            "pre_state,expected_post_state,status,created_at,updated_at "
            "FROM cdc.ddl_replay_journal "
            "WHERE source_identity LIKE 'cdc-harness-source#server-id=%' "
            "ORDER BY event_start_position;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        )
        rows = [line.split("\t") for line in output.splitlines() if line.strip()]
        if len(rows) != 1:
            raise HarnessError(f"expected exactly one immutable journal row, got {output}")
        if len(rows[0]) != 15:
            raise HarnessError(f"immutable journal row column mismatch count={len(rows[0])} output={output!r}")
        names = [
            "source_identity",
            "source_server_id",
            "binlog_file",
            "event_start_position",
            "event_end_position",
            "schema_name",
            "raw_sql",
            "transformation_version",
            "generated_sql",
            "canonical_ast",
            "pre_state",
            "expected_post_state",
            "status",
            "created_at",
            "updated_at",
        ]
        return dict(zip(names, rows[0], strict=True))

    def replace_journal_row(self, row: dict[str, str]) -> None:
        assert self.target
        self.admin_sql(
            self.target,
            "DELETE FROM cdc.ddl_replay_journal "
            f"WHERE source_identity={sql_literal(row['source_identity'])} "
            f"AND binlog_file={sql_literal(row['binlog_file'])} "
            f"AND event_start_position={row['event_start_position']};",
        )
        self.admin_sql(
            self.target,
            "INSERT INTO cdc.ddl_replay_journal "
            "(source_identity,source_server_id,binlog_file,event_start_position,event_end_position,"
            "schema_name,raw_sql,transformation_version,generated_sql,canonical_ast,pre_state,"
            "expected_post_state,status) VALUES ("
            f"{sql_literal(row['source_identity'])},{row['source_server_id']},"
            f"{sql_literal(row['binlog_file'])},{row['event_start_position']},{row['event_end_position']},"
            f"{sql_literal(row['schema_name'])},{sql_literal(row['raw_sql'])},"
            f"{sql_literal(row['transformation_version'])},{sql_literal(row['generated_sql'])},"
            f"{sql_literal(row['canonical_ast'])},{sql_literal(row['pre_state'])},"
            f"{sql_literal(row['expected_post_state'])},{sql_literal(row['status'])});",
        )

    def prepare_checkpointed_ddl(self) -> tuple[Coordinate, Coordinate, dict[str, str]]:
        assert self.source and self.target
        self.setup_accounts_table()
        start = self.coordinate()
        self.write_checkpoint(start)
        self.admin_sql(self.source, "CREATE INDEX idx_accounts_email ON accounts (email);")
        self.admin_sql(self.source, "INSERT INTO accounts VALUES (1, 'one@example.test', 'one');")
        final_stop = self.coordinate()
        result = self.run_stream(start, final_stop)
        require_success(result, "journal mismatch baseline DDL")
        row = self.journal_full_row()
        if row["status"] != "checkpointed":
            raise HarnessError(f"baseline journal row is not checkpointed: {row}")
        self.write_checkpoint(start)
        return start, final_stop, row

    def assert_reuse_rejected(
        self,
        scenario: str,
        start: Coordinate,
        final_stop: Coordinate,
        row: dict[str, str],
        field: str,
    ) -> None:
        assert self.target
        baseline_indexes = self.query(
            self.target, "SHOW INDEX FROM accounts;", user=TARGET_USER, password=TARGET_PASSWORD
        )
        baseline_rows = self.query(
            self.target,
            "SELECT COUNT(*) FROM accounts;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        self.admin_sql(
            self.target,
            "SET GLOBAL general_log=OFF; TRUNCATE TABLE mysql.general_log; "
            "SET GLOBAL log_output='TABLE'; SET GLOBAL general_log=ON;",
        )
        self.replace_journal_row(row)
        inserted_row = self.journal_full_row()
        generated_fields = {"created_at", "updated_at"}
        for field_name, expected_value in row.items():
            if field_name in generated_fields:
                continue
            if inserted_row[field_name] != expected_value:
                raise HarnessError(
                    f"{scenario} replacement row mismatch field={field_name}: "
                    f"expected={expected_value!r} actual={inserted_row[field_name]!r}"
                )
        result = self.run_stream(start, final_stop)
        mutation_attempts = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM mysql.general_log WHERE user_host LIKE 'cdc_stream%' "
            "AND command_type IN ('Query','Prepare','Execute') "
            "AND UPPER(argument) REGEXP '^[[:space:]]*(INSERT|UPDATE|DELETE|REPLACE|CREATE|ALTER|DROP|TRUNCATE|RENAME)[[:space:]]';",
        ).strip()
        self.admin_sql(self.target, "SET GLOBAL general_log=OFF;")
        output = f"{result.stdout}\\n{result.stderr}".lower()
        if result.returncode == 0 or "identity mismatch" not in output or field not in output:
            raise HarnessError(
                f"{scenario} did not reject immutable-field reuse field={field}: "
                f"exit={result.returncode} output={result.stdout} {result.stderr}"
            )
        if mutation_attempts != "0":
            raise HarnessError(
                f"{scenario} attempted target mutation before identity rejection: attempts={mutation_attempts}"
            )
        if self.query(self.target, "SHOW INDEX FROM accounts;", user=TARGET_USER, password=TARGET_PASSWORD) != baseline_indexes:
            raise HarnessError(f"{scenario} mutated target schema before identity rejection")
        if self.query(self.target, "SELECT COUNT(*) FROM accounts;", user=TARGET_USER, password=TARGET_PASSWORD).strip() != baseline_rows:
            raise HarnessError(f"{scenario} mutated target rows before identity rejection")
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != start.file or int(checkpoint.get("source_position", 0)) != start.position:
            raise HarnessError(f"{scenario} advanced checkpoint after identity rejection: {checkpoint}")
        retained = self.journal_full_row()
        if retained != inserted_row:
            raise HarnessError(
                f"{scenario} changed inserted journal row: expected={inserted_row} retained={retained}"
            )
        print(f"{scenario}_blocked identity_mismatch={field} no_overtake=true evidence_retained=true")

    def run_journal_mismatch_scenario(self, scenario: str) -> None:
        assert self.source and self.target
        if scenario == "pre-state-drift":
            self.setup_accounts_table()
            start = self.coordinate()
            self.write_checkpoint(start)
            self.admin_sql(self.source, "CREATE INDEX idx_accounts_email ON accounts (email);")
            self.admin_sql(self.source, "INSERT INTO accounts VALUES (1, 'one@example.test', 'one');")
            final_stop = self.coordinate()
            prepared = self.run_stream(start, final_stop, integration_failpoint="prepare-failure")
            if prepared.returncode == 0 or "cdc_integration_failpoint" not in f"{prepared.stdout}\\n{prepared.stderr}":
                raise HarnessError(f"pre-state-drift did not retain prepared journal evidence: {prepared}")
            self.assert_recovery_state(start, expected_status="prepared", expected_index=False, expected_rows="0")
            self.admin_sql(self.target, "CREATE INDEX idx_accounts_external ON accounts (email);")
            blocked = self.run_stream(start, final_stop)
            output = f"{blocked.stdout}\\n{blocked.stderr}".lower()
            if blocked.returncode == 0 or "pre-state mismatch" not in output:
                raise HarnessError(f"pre-state-drift did not reject external inventory drift: {blocked}")
            rows = self.ddl_journal_rows()
            if len(rows) != 1 or rows[0][0] != "blocked" or any(int(value) <= 0 for value in rows[0][1:4]):
                raise HarnessError(f"pre-state-drift lost immutable evidence: {rows}")
            indexes = self.query(self.target, "SHOW INDEX FROM accounts;", user=TARGET_USER, password=TARGET_PASSWORD)
            if "idx_accounts_external" not in indexes or "idx_accounts_email" in indexes:
                raise HarnessError(f"pre-state-drift target state crossed DDL boundary: {indexes}")
            if self.query(self.target, "SELECT COUNT(*) FROM accounts;", user=TARGET_USER, password=TARGET_PASSWORD).strip() != "0":
                raise HarnessError("pre-state-drift applied later DML after blocked reconciliation")
            checkpoint = self.checkpoint()
            if checkpoint.get("source_file") != start.file or int(checkpoint.get("source_position", 0)) != start.position:
                raise HarnessError(f"pre-state-drift advanced checkpoint: {checkpoint}")
            print("pre-state-drift_blocked pre-state_mismatch=true evidence_retained=true no_overtake=true")
            return

        if scenario == "checkpoint-mismatch":
            self.setup_accounts_table()
            start = self.coordinate()
            self.write_checkpoint(start)
            self.admin_sql(self.source, "CREATE INDEX idx_accounts_email ON accounts (email);")
            self.admin_sql(self.source, "INSERT INTO accounts VALUES (1, 'one@example.test', 'one');")
            final_stop = self.coordinate()
            barrier_dir = self.tempdir / "checkpoint-mismatch-barrier"
            process, _log = self.start_stream(
                start,
                final_stop,
                integration_failpoint="target-connection-loss",
                barrier_dir=barrier_dir,
                label="checkpoint-mismatch",
            )
            self.wait_for_barrier(process, barrier_dir, "after-target-operation-before-journal-applied")
            self.assert_recovery_state(start, expected_status="prepared", expected_index=True, expected_rows="0")
            journal_row = self.journal_full_row()
            wrong = Coordinate(
                journal_row["binlog_file"],
                int(journal_row["event_start_position"]) + 1,
            )
            self.write_checkpoint(wrong)
            self.release_barrier(barrier_dir, "after-target-operation-before-journal-applied")
            blocked = self.finish_stream(process)
            blocked_output = f"{blocked.stdout}\\n{blocked.stderr}".lower()
            if blocked.returncode == 0 or "checkpoint predecessor mismatch" not in blocked_output:
                raise HarnessError(f"checkpoint-mismatch did not block predecessor disagreement: {blocked}")
            rows = self.ddl_journal_rows()
            if len(rows) != 1 or rows[0][0] != "applied" or any(int(value) <= 0 for value in rows[0][1:4]):
                raise HarnessError(f"checkpoint-mismatch changed journal evidence: {rows}")
            if self.query(self.target, "SHOW INDEX FROM accounts;", user=TARGET_USER, password=TARGET_PASSWORD).count("idx_accounts_email") != 1:
                raise HarnessError("checkpoint-mismatch unexpectedly changed target DDL state")
            if self.query(self.target, "SELECT COUNT(*) FROM accounts;", user=TARGET_USER, password=TARGET_PASSWORD).strip() != "0":
                raise HarnessError("checkpoint-mismatch applied later DML after predecessor rejection")
            checkpoint = self.checkpoint()
            if checkpoint.get("source_file") != wrong.file or int(checkpoint.get("source_position", 0)) != wrong.position:
                raise HarnessError(f"checkpoint-mismatch advanced checkpoint: {checkpoint}")
            print("checkpoint-mismatch_blocked checkpoint_predecessor_mismatch=true evidence_retained=true no_overtake=true")
            return

        start, final_stop, row = self.prepare_checkpointed_ddl()
        row["status"] = "prepared"
        if scenario == "coordinate-reuse":
            row["source_server_id"] = str(int(row["source_server_id"]) + 1)
            self.assert_reuse_rejected(scenario, start, final_stop, row, "source_server_id")
        elif scenario == "raw-sql-reuse":
            row["raw_sql"] = row["raw_sql"] + " /* reused coordinate */"
            self.assert_reuse_rejected(scenario, start, final_stop, row, "raw_sql")
        elif scenario == "end-position-reuse":
            row["event_end_position"] = str(int(row["event_end_position"]) + 1)
            self.assert_reuse_rejected(scenario, start, final_stop, row, "event_end_position")
        else:
            raise HarnessError(f"unknown journal mismatch scenario: {scenario}")

    def run_recovery_scenario(self, scenario: str) -> None:
        assert self.source and self.target
        self.setup_accounts_table()
        start = self.coordinate()
        self.write_checkpoint(start)
        self.admin_sql(self.source, "CREATE INDEX idx_accounts_email ON accounts (email);")
        self.admin_sql(self.source, "INSERT INTO accounts VALUES (1, 'one@example.test', 'one');")
        final_stop = self.coordinate()

        crashed = self.run_stream(start, final_stop, integration_failpoint=scenario)
        output = f"{crashed.stdout}\\n{crashed.stderr}"
        if crashed.returncode == 0 or "cdc_integration_failpoint" not in output:
            raise HarnessError(f"{scenario} did not terminate at its deterministic failpoint: {output}")

        crash_status = "prepared" if scenario in {"prepare-failure", "post-ddl-pre-applied"} else "applied"
        crash_index = scenario != "prepare-failure"
        self.assert_recovery_state(
            start,
            expected_status=crash_status,
            expected_index=crash_index,
            expected_rows="0",
        )

        restarted = self.run_stream(start, final_stop)
        if scenario == "prepare-failure":
            if restarted.returncode == 0 or "semantic reconciliation blocked" not in f"{restarted.stdout}\n{restarted.stderr}".lower():
                raise HarnessError(f"{scenario} restart did not stop at the blocking boundary: {restarted}")
            self.assert_recovery_state(
                start,
                expected_status="blocked",
                expected_index=False,
                expected_rows="0",
            )
            print(f"{scenario}_blocked blocking=manual-resolution no_overtake coordinate={start.file}:{start.position}")
            return

        require_success(restarted, f"{scenario} restart")
        if scenario == "post-ddl-pre-applied" and "cdc_ddl_reconcile_prepared" not in restarted.stdout:
            raise HarnessError(f"{scenario} restart did not report prepared-state reconciliation")
        if scenario in {"applied-pre-checkpoint", "checkpoint-transaction"} and "cdc_ddl_checkpoint_only" not in restarted.stdout:
            raise HarnessError(f"{scenario} restart did not use checkpoint-only recovery")
        self.assert_recovery_state(
            final_stop,
            expected_status="checkpointed",
            expected_index=True,
            expected_rows="1",
        )
        pending = self.query(
            self.target,
            "SELECT COUNT(*) FROM cdc.ddl_replay_journal WHERE status IN ('translation_pending','blocked');",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if pending != "0":
            raise HarnessError(f"{scenario} left unresolved DDL journal debt: {pending}")
        print(f"{scenario}_converged convergence=complete coordinate={final_stop.file}:{final_stop.position}")

    def wait_for_target_count(self, expected: str, timeout: float = 60.0) -> None:
        assert self.target
        deadline = time.monotonic() + timeout
        last = ""
        while time.monotonic() < deadline:
            try:
                last = self.query(
                    self.target,
                    "SELECT COUNT(*) FROM accounts;",
                    user=TARGET_USER,
                    password=TARGET_PASSWORD,
                ).strip()
            except HarnessError:
                time.sleep(0.25)
                continue
            if last == expected:
                return
            time.sleep(0.25)
        raise HarnessError(f"target row count did not converge to {expected}: {last}")

    def wait_for_checkpoint(self, coordinate: Coordinate, timeout: float = 60.0) -> None:
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                last = self.checkpoint()
            except HarnessError:
                time.sleep(0.25)
                continue
            if last.get("source_file") == coordinate.file and int(last.get("source_position", 0)) >= coordinate.position:
                return
            time.sleep(0.25)
        raise HarnessError(f"checkpoint did not reach {coordinate.file}:{coordinate.position}: {last}")

    def run_connection_loss_scenario(self, scenario: str) -> None:
        assert self.source and self.target
        self.setup_accounts_table()
        start = self.coordinate()
        self.write_checkpoint(start)
        barrier_dir = self.tempdir / f"{scenario}-barrier"

        if scenario == "source-connection-loss":
            self.admin_sql(self.source, "INSERT INTO accounts VALUES (1, 'one@example.test', 'one');")
            first_stop = self.coordinate()
            process, _log = self.start_stream(
                start,
                integration_failpoint=scenario,
                max_reconnects=6,
                barrier_dir=barrier_dir,
                label=scenario,
            )
            self.wait_for_barrier(process, barrier_dir, "after-committed-event")
            run(["docker", "kill", self.source.container])
            run(["docker", "start", self.source.container])
            self.source = self.refresh_endpoint(self.source)
            wait_for_sql(self.source, self.ca_file)
            self.release_barrier(barrier_dir, "after-committed-event")
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if "cdc_stream_reconnect_start" in self.process_output(process):
                    break
                if process.poll() is not None:
                    raise HarnessError(f"source loss stream exited before reconnect: {self.process_output(process)}")
                time.sleep(0.1)
            else:
                raise HarnessError(f"source loss did not enter reconnect loop: {self.process_output(process)}")
            self.admin_sql(self.source, "INSERT INTO accounts VALUES (2, 'two@example.test', 'two');")
            second_stop = self.coordinate()
            self.wait_for_target_count("2")
            self.wait_for_checkpoint(second_stop)
            output = self.process_output(process)
            process.terminate()
            process.wait(timeout=30)
            if "cdc_stream_reconnect_start" not in output:
                raise HarnessError(f"source reconnect evidence missing: {output}")
            checkpoint = self.checkpoint()
            if checkpoint.get("source_file") != second_stop.file or int(checkpoint.get("source_position", 0)) < second_stop.position:
                raise HarnessError(f"source reconnect checkpoint did not advance after recovery: {checkpoint}")
            if not coordinate_is_after(second_stop, first_stop):
                raise HarnessError(f"source event boundary did not advance: {first_stop} -> {second_stop}")
            journal_count = self.query(
                self.target,
                "SELECT COUNT(*) FROM cdc.ddl_replay_journal;",
                user=TARGET_USER,
                password=TARGET_PASSWORD,
            ).strip()
            if journal_count != "0":
                raise HarnessError(f"source loss unexpectedly created journal rows: {journal_count}")
            print(f"{scenario}_converged reconnect=observed target_rows=2 checkpoint={second_stop.file}:{second_stop.position}")
            return

        if scenario != "target-connection-loss":
            raise HarnessError(f"unknown connection-loss scenario: {scenario}")

        self.admin_sql(self.source, "CREATE INDEX idx_accounts_email ON accounts (email);")
        final_stop = self.coordinate()
        process, _log = self.start_stream(
            start,
            final_stop,
            integration_failpoint=scenario,
            barrier_dir=barrier_dir,
            label=scenario,
        )
        self.wait_for_barrier(process, barrier_dir, "after-target-operation-before-journal-applied")
        journal = self.ddl_journal_rows()
        if len(journal) != 1 or journal[0][0] != "prepared":
            raise HarnessError(f"target loss journal was not prepared before interruption: {journal}")
        indexes = self.query(self.target, "SHOW INDEX FROM accounts;", user=TARGET_USER, password=TARGET_PASSWORD)
        if "idx_accounts_email" not in indexes:
            raise HarnessError(f"target loss barrier did not follow target DDL mutation: {indexes}")
        self.assert_recovery_state(start, expected_status="prepared", expected_index=True, expected_rows="0")
        pre_restart_checkpoint = self.checkpoint()
        run(["docker", "restart", self.target.container])
        self.target = self.refresh_endpoint(self.target)
        self.release_barrier(barrier_dir, "after-target-operation-before-journal-applied")
        crashed = self.finish_stream(process)
        if crashed.returncode == 0:
            raise HarnessError("target loss stream reported success after target connection loss")
        wait_for_sql(self.target, self.ca_file)
        if self.checkpoint() != pre_restart_checkpoint:
            raise HarnessError("target loss advanced or regressed checkpoint before restart")
        restarted = self.run_stream(start, final_stop)
        require_success(restarted, "target-connection-loss restart")
        self.assert_recovery_state(final_stop, expected_status="checkpointed", expected_index=True, expected_rows="0")
        print(f"{scenario}_converged journal=checkpointed checkpoint={final_stop.file}:{final_stop.position}")

    def run_row_conflict_source_row_migration(self) -> None:
        assert self.target
        self.admin_sql(self.target, "DROP TRIGGER cdc.row_conflicts_update_guard;")
        self.admin_sql(
            self.target,
            "ALTER TABLE cdc.row_conflicts "
            "DROP INDEX row_conflicts_source_row_status, "
            "DROP COLUMN source_row_identity;",
        )
        conflict_identity = self.conflict_identity(
            "binlog.000001", 4, "accounts", ["1"], operation="insert"
        )
        self.admin_sql(
            self.target,
            "INSERT INTO cdc.row_conflicts "
            "(conflict_identity,source_identity,source_server_id,source_file,"
            "source_start_position,source_end_position,schema_name,table_name,operation,"
            "source_primary_key_json,duplicate_index,duplicate_owner_primary_key_json,"
            "error_code,error_text,first_observed_at_ms,last_observed_at_ms,attempt_count,status) "
            f"VALUES ({sql_literal(conflict_identity)},{sql_literal(SOURCE_IDENTITY)},101,"
            "'binlog.000001',4,8,'globalcomix','accounts','insert','[\"1\"]',"
            "NULL,NULL,1062,'legacy conflict',1,1,1,'unresolved');",
        )
        self.admin_sql_file(
            self.target,
            self.repo / "docs/row-conflicts-source-row-identity-migration.sql",
        )
        expected_identity = self.source_row_identity("accounts", ["1"])
        migrated = self.admin_query(
            self.target,
            "SELECT source_row_identity FROM cdc.row_conflicts "
            "WHERE conflict_identity=" + sql_literal(conflict_identity) + ";",
        ).strip()
        if migrated != expected_identity:
            raise HarnessError(
                f"source-row migration backfill mismatch: expected={expected_identity} actual={migrated}"
            )
        index_columns = self.admin_query(
            self.target,
            "SELECT column_name FROM information_schema.statistics "
            "WHERE table_schema='cdc' AND table_name='row_conflicts' "
            "AND index_name='row_conflicts_source_row_status' ORDER BY seq_in_index;",
        ).strip()
        if index_columns != "source_row_identity\nstatus":
            raise HarnessError(f"source-row migration index mismatch: {index_columns!r}")
        self.assert_admin_sql_rejected(
            self.target,
            "UPDATE cdc.row_conflicts SET source_identity='mutated' "
            f"WHERE conflict_identity={sql_literal(conflict_identity)};",
            "row conflict identity is immutable",
        )
        print(
            "row-conflict-source-row-migration_ok existing_rows_backfilled=true "
            "lookup_index=true identity_immutable=true"
        )

    def assert_foreign_keys_enabled(self) -> None:
        assert self.source and self.target
        for endpoint, label in ((self.source, "source"), (self.target, "target")):
            checks = self.admin_query(endpoint, "SELECT @@FOREIGN_KEY_CHECKS;").strip()
            if checks != "1":
                raise HarnessError(f"{label} foreign-key checks were not enabled: {checks}")

    def setup_sync_accounts(self, table: str = "sync_accounts") -> None:
        assert self.source and self.target
        schema = (
            f"DROP TABLE IF EXISTS {table}; "
            f"CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL, "
            f"payload VARCHAR(64) NOT NULL, UNIQUE KEY uq_{table}_email (email)) ENGINE=InnoDB;"
        )
        self.admin_sql(self.source, schema)
        self.admin_sql(self.target, schema)
        self.assert_foreign_keys_enabled()

    def run_sync_progress_least_privilege(self) -> None:
        assert self.source and self.target
        self.setup_sync_accounts()
        self.admin_sql(
            self.source,
            "INSERT INTO sync_accounts VALUES (1, 'one@example.test', 'one');",
        )
        grants = normalize_grants(
            self.admin_query(self.target, "SHOW GRANTS FOR 'cdc_sync'@'%';")
        )
        upper_grants = [grant.upper() for grant in grants]
        if not any(grant.startswith("GRANT CREATE ON CDC.*") for grant in upper_grants):
            raise HarnessError(f"sync CDC schema grant missing: {grants!r}")
        if not any(
            grant.startswith("GRANT SELECT, INSERT, UPDATE ON CDC.SYNC_RUNS")
            for grant in upper_grants
        ):
            raise HarnessError(f"sync progress-table grant missing: {grants!r}")
        if any(
            grant.startswith("GRANT ")
            and " ON CDC.* " in grant
            and any(privilege in grant.split(" ON ", 1)[0] for privilege in ("ALTER", "DROP", "DELETE"))
            for grant in upper_grants
        ):
            raise HarnessError(f"sync identity has excessive CDC grants: {grants!r}")

        result = self.run_sync(
            tables=["sync_accounts"],
            run_id="sync-progress-least-privilege",
            chunk_size=1,
        )
        require_success(result, "sync progress least privilege")
        target_row = self.admin_query(
            self.target,
            "SELECT id,email,payload FROM sync_accounts;",
        ).strip()
        if target_row != "1	one@example.test	one":
            raise HarnessError(f"unified sync did not copy exact row: {target_row!r}")
        progress = self.admin_query(
            self.target,
            "SELECT stage,table_name,rows_scanned,inserts_applied,status "
            "FROM cdc.sync_runs WHERE run_id='sync-progress-least-privilege' "
            "ORDER BY FIELD(stage,'prerequisite_schema','rows','final_constraints');",
        ).splitlines()
        expected_progress = [
            "prerequisite_schema	sync_accounts	0	0	complete",
            "rows	sync_accounts	1	1	complete",
            "final_constraints	sync_accounts	0	0	complete",
        ]
        if progress != expected_progress:
            raise HarnessError(f"unexpected unified sync progress: {progress!r}")
        print(
            "sync_progress_least_privilege_ok rows=1 progress_rows=3 "
            "cdc_schema_create_only=true"
        )

    def run_sync_composite_enum_primary_key(self) -> None:
        assert self.source and self.target
        create_table = (
            "DROP TABLE IF EXISTS comics_top_stats; "
            "CREATE TABLE comics_top_stats ("
            "comic_id INT UNSIGNED NOT NULL, "
            "statistic ENUM('views','popularity','likes','purchases','loved','rising') NOT NULL, "
            "value_365_days FLOAT UNSIGNED NOT NULL, "
            "PRIMARY KEY (comic_id, statistic)"
            ") ENGINE=InnoDB;"
        )
        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, create_table)
        self.admin_sql(
            self.source,
            "INSERT INTO comics_top_stats VALUES "
            "(13553, 'views', 4895), "
            "(13553, 'popularity', 9.02522), "
            "(13553, 'loved', 0.00989477);",
        )
        self.admin_sql(
            self.target,
            "INSERT INTO comics_top_stats VALUES "
            "(13553, 'views', 4891), "
            "(13553, 'popularity', 9.02522);",
        )
        run_id = "sync-composite-enum-primary-key"
        result = self.run_sync(
            tables=["comics_top_stats"],
            run_id=run_id,
            chunk_size=2,
        )
        require_success(result, "sync composite ENUM primary key")
        rows = self.query(
            self.target,
            "SELECT comic_id,statistic,value_365_days FROM comics_top_stats "
            "ORDER BY comic_id,statistic;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).splitlines()
        expected_rows = [
            "13553	views	4895",
            "13553	popularity	9.02522",
            "13553	loved	0.00989477",
        ]
        if rows != expected_rows:
            raise HarnessError(f"composite ENUM rows did not converge: {rows!r}")
        progress = self.admin_query(
            self.target,
            "SELECT status,last_primary_key_json,chunks,rows_scanned,"
            "inserts_applied,updates_applied,deletes_applied FROM cdc.sync_runs "
            f"WHERE run_id='{run_id}' AND stage='rows' AND table_name='comics_top_stats';",
        ).strip()
        if progress != 'complete	["13553","loved"]	2	3	1	1	0':
            raise HarnessError(f"composite ENUM progress is wrong: {progress!r}")
        print(
            "sync_composite_enum_primary_key_ok "
            "rows_scanned=3 inserts=1 updates=1 deletes=0"
        )

    def run_sync_fk_parent_convergence(self, update_existing_child: bool = False) -> None:
        assert self.source and self.target
        run_id = "sync-fk-parent-update" if update_existing_child else "sync-fk-parent-insert"
        for endpoint in (self.source, self.target):
            self.admin_sql(
                endpoint,
                "DROP TABLE IF EXISTS guests; DROP TABLE IF EXISTS utms; "
                "CREATE TABLE utms ("
                "id INT UNSIGNED NOT NULL PRIMARY KEY, "
                "utm_hash VARCHAR(64) NOT NULL UNIQUE"
                ") ENGINE=InnoDB; "
                "CREATE TABLE guests ("
                "guest_id BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
                "guest_hash CHAR(40) NOT NULL UNIQUE, "
                "utm_id INT UNSIGNED NULL, "
                "CONSTRAINT fk_guests_utm_id FOREIGN KEY (utm_id) REFERENCES utms(id)"
                ") ENGINE=InnoDB;",
            )
        self.admin_sql(
            self.source,
            "INSERT INTO utms VALUES (184041, "
            "'42f66cafa34b0fb11f329298c627bc1b9fa233d772b5bb5cd621f1bbe8dced6d'); "
            "INSERT INTO guests VALUES (87308589, "
            "'6ee3278e-f4e0-4242-bd66-1342633d84f1G4Cd', 184041);",
        )
        if update_existing_child:
            self.admin_sql(
                self.target,
                "INSERT INTO utms VALUES (1, 'existing-target-parent'); "
                "INSERT INTO guests VALUES (87308589, "
                "'6ee3278e-f4e0-4242-bd66-1342633d84f1G4Cd', 1);",
            )
        result = self.run_sync(
            tables=["utms", "guests"],
            run_id=run_id,
            chunk_size=1,
        )
        require_success(result, "sync FK parent convergence")
        parent = self.query(
            self.target,
            "SELECT id,utm_hash FROM utms WHERE id=184041;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        child = self.query(
            self.target,
            "SELECT guest_id,guest_hash,utm_id FROM guests WHERE guest_id=87308589;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        progress = self.admin_query(
            self.target,
            "SELECT status,last_primary_key_json FROM cdc.sync_runs "
            f"WHERE run_id='{run_id}' AND stage='rows' AND table_name='guests';",
        ).strip()
        if not parent.startswith("184041	42f66c"):
            raise HarnessError(f"FK parent did not converge: {parent!r}")
        if child != "87308589	6ee3278e-f4e0-4242-bd66-1342633d84f1G4Cd	184041":
            raise HarnessError(f"FK child did not converge: {child!r}")
        if progress != 'complete	["87308589"]':
            raise HarnessError(f"FK sync progress mismatch: {progress!r}")
        operation = "update" if update_existing_child else "insert"
        print(f"sync_fk_parent_converged operation={operation} constraints_restored=true")

    def run_sync_fk_parent_stale_unique_owner(self) -> None:
        assert self.source and self.target
        for endpoint in (self.source, self.target):
            self.admin_sql(
                endpoint,
                "DROP TABLE IF EXISTS favorites; DROP TABLE IF EXISTS users; "
                "CREATE TABLE users ("
                "id INT UNSIGNED NOT NULL PRIMARY KEY, "
                "email VARCHAR(255) NULL UNIQUE, "
                "name VARCHAR(255) NULL UNIQUE, "
                "is_deleted TINYINT(1) NOT NULL, "
                "UNIQUE KEY uq_users_id_name (id, name)"
                ") ENGINE=InnoDB; "
                "CREATE TABLE favorites ("
                "id INT UNSIGNED NOT NULL PRIMARY KEY, "
                "user_id INT UNSIGNED NOT NULL, "
                "user_name VARCHAR(255) NOT NULL, "
                "CONSTRAINT fk_favorites_user FOREIGN KEY (user_id, user_name) "
                "REFERENCES users(id, name)"
                ") ENGINE=InnoDB;",
            )
        self.admin_sql(
            self.source,
            "INSERT INTO users VALUES "
            "(1, 'deleted-1@example.test', 'deleted-user-1', 1), "
            "(2, 'live@example.test', 'LiveUser', 0); "
            "INSERT INTO favorites VALUES (10, 2, 'LiveUser');",
        )
        self.admin_sql(
            self.target,
            "INSERT INTO users VALUES (1, 'live@example.test', 'StaleOwner', 0);",
        )
        run_id = "sync-fk-parent-stale-unique-owner"
        result = self.run_sync(
            tables=["users", "favorites"],
            run_id=run_id,
            chunk_size=1,
        )
        require_success(result, "sync stale unique owner")
        parents = self.query(
            self.target,
            "SELECT id,email,name,is_deleted FROM users ORDER BY id;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).splitlines()
        child = self.query(
            self.target,
            "SELECT id,user_id,user_name FROM favorites;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        expected_parents = [
            "1	deleted-1@example.test	deleted-user-1	1",
            "2	live@example.test	LiveUser	0",
        ]
        if parents != expected_parents:
            raise HarnessError(f"stale unique owner did not converge: {parents!r}")
        if child != "10	2	LiveUser":
            raise HarnessError(f"child did not converge after parent displacement: {child!r}")
        print("sync_fk_parent_stale_unique_owner_ok constraints_restored=true")

    def run_sync_wide_update(self) -> None:
        assert self.source and self.target
        payload_columns = [f"value_{index}" for index in range(1, 256)]
        column_definitions = ", ".join(
            f"{column} CHAR(1) NOT NULL" for column in payload_columns
        )
        columns = ["id", "parent_id", *payload_columns]
        quoted_columns = ",".join(f"`{column}`" for column in columns)
        for endpoint in (self.source, self.target):
            self.admin_sql(
                endpoint,
                "DROP TABLE IF EXISTS wide_children; DROP TABLE IF EXISTS wide_parents; "
                "CREATE TABLE wide_parents (id INT UNSIGNED NOT NULL PRIMARY KEY, "
                "parent_hash VARCHAR(64) NOT NULL UNIQUE) ENGINE=InnoDB; "
                "CREATE TABLE wide_children (id INT UNSIGNED NOT NULL PRIMARY KEY, "
                "parent_id INT UNSIGNED NOT NULL, "
                f"{column_definitions}, "
                "CONSTRAINT fk_wide_children_parent FOREIGN KEY (parent_id) "
                "REFERENCES wide_parents(id)) ENGINE=InnoDB;",
            )
        self.admin_sql(
            self.source,
            "INSERT INTO wide_parents VALUES (1, 'existing'), (184041, 'repaired');",
        )
        self.admin_sql(self.target, "INSERT INTO wide_parents VALUES (1, 'existing');")

        def values(row_id: int, parent_id: int, payload: str) -> str:
            fields = [str(row_id), str(parent_id), *([f"'{payload}'"] * 255)]
            return f"({','.join(fields)})"

        source_rows = ",".join(
            values(row_id, 1 if row_id <= 127 else 184041, "s")
            for row_id in range(1, 130)
        )
        target_rows = ",".join(values(row_id, 1, "t") for row_id in range(1, 130))
        self.admin_sql(
            self.source,
            f"INSERT INTO wide_children ({quoted_columns}) VALUES {source_rows};",
        )
        self.admin_sql(
            self.target,
            f"INSERT INTO wide_children ({quoted_columns}) VALUES {target_rows};",
        )
        run_id = "sync-wide-update"
        result = self.run_sync(
            tables=["wide_parents", "wide_children"],
            run_id=run_id,
            chunk_size=129,
            timeout=240,
        )
        require_success(result, "sync wide update")
        drift = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM wide_children WHERE "
            "value_1 <> 's' OR (id <= 127 AND parent_id <> 1) OR "
            "(id >= 128 AND parent_id <> 184041);",
        ).strip()
        progress = self.admin_query(
            self.target,
            "SELECT status,last_primary_key_json,updates_applied,chunks "
            "FROM cdc.sync_runs "
            f"WHERE run_id='{run_id}' AND stage='rows' AND table_name='wide_children';",
        ).strip()
        if drift != "0":
            raise HarnessError(f"wide sync left divergent rows: {drift}")
        if progress != 'complete	["129"]	129	1':
            raise HarnessError(f"wide sync progress mismatch: {progress!r}")
        print("sync_wide_update_ok rows=129 updates=129 chunks=1")

    def sync_progress_snapshot(
        self,
        run_id: str,
        *,
        stage: str | None = None,
        include_run_spec: bool = True,
    ) -> str:
        assert self.target
        columns = [
            "HEX(run_id)",
            "HEX(stage)",
            "HEX(table_name)",
        ]
        if include_run_spec:
            columns.append("HEX(run_spec_json)")
        columns.extend(
            [
                "IF(last_primary_key_json IS NULL, '<NULL>', HEX(last_primary_key_json))",
                "chunks",
                "rows_scanned",
                "inserts_applied",
                "updates_applied",
                "deletes_applied",
                "HEX(status)",
                "IF(last_error IS NULL, '<NULL>', HEX(last_error))",
                "DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f')",
                "DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s.%f')",
                "IF(completed_at IS NULL, '<NULL>', DATE_FORMAT(completed_at, '%Y-%m-%d %H:%i:%s.%f'))",
            ]
        )
        filters = [f"run_id={sql_literal(run_id)}"]
        if stage is not None:
            filters.append(f"stage={sql_literal(stage)}")
        sql = (
            f"SELECT {','.join(columns)} FROM cdc.sync_runs "
            f"WHERE {' AND '.join(filters)} "
            "ORDER BY FIELD(stage,'prerequisite_schema','rows','final_constraints'),table_name;"
        )
        return self.admin_query(self.target, sql).strip()

    def sync_run_spec_and_sha256(self, run_id: str) -> tuple[str, str]:
        assert self.target
        output = self.admin_query(
            self.target,
            "SELECT run_spec_json,LOWER(SHA2(run_spec_json,256)) "
            "FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} ORDER BY stage,table_name LIMIT 1;",
        ).strip()
        fields = output.split("\t", 1)
        if len(fields) != 2:
            raise HarnessError(f"unexpected sync run specification evidence: {output!r}")
        return fields[0], fields[1]

    def sync_run_spec_migration_audit(self, result: CommandResult) -> dict:
        audits = []
        for line in "\n".join((result.stdout, result.stderr)).splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if value.get("event") == "sync_run_spec_migration":
                audits.append(value)
        if len(audits) != 1:
            raise HarnessError(
                "expected exactly one sync_run_spec_migration audit, "
                f"found {len(audits)} in stdout={result.stdout!r} stderr={result.stderr!r}"
            )
        return audits[0]

    def stop_sync_process(self, process: subprocess.Popen[str]) -> None:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=30)
        log = getattr(process, "_cdc_log", None)
        if log is not None:
            log.close()

    def run_sync_authorized_additive_spec_migration(self) -> None:
        assert self.source and self.target
        run_id = "sync-authorized-additive-spec-migration"
        table_a = "migration_a_started"
        table_z = "migration_z_changed"
        tables = [table_a, table_z]

        (
            old_spec,
            old_sha256,
            before_wrong_hash,
            prerequisite_before_migration,
            preexisting_row_count,
        ) = self.prepare_interrupted_run_spec_migration(
            run_id,
            table_a,
            table_z,
            tables,
        )
        self.assert_wrong_hash_run_spec_migration_no_write(
            run_id,
            table_z,
            tables,
            before_wrong_hash,
        )
        _, current_sha256 = self.execute_successful_run_spec_migration(
            run_id,
            table_a,
            table_z,
            tables,
            old_spec,
            old_sha256,
            prerequisite_before_migration,
            preexisting_row_count,
        )
        self.assert_idempotent_run_spec_migration(
            run_id,
            tables,
            old_sha256,
            current_sha256,
        )
        self.assert_changed_table_row_progress_rejection(
            run_id,
            table_z,
            tables,
            current_sha256,
        )

        print(
            "sync_authorized_additive_spec_migration_ok "
            f"run_id={run_id} old_sha256={old_sha256} new_sha256={current_sha256} "
            f"migrated_rows={preexisting_row_count} idempotent=true "
            "changed_table_row_progress_rejected=true"
        )

    def prepare_interrupted_run_spec_migration(
        self,
        run_id: str,
        table_a: str,
        table_z: str,
        tables: list[str],
    ) -> tuple[str, str, str, str, int]:
        assert self.source and self.target
        create_tables = (
            f"DROP TABLE IF EXISTS {table_a}; "
            f"DROP TABLE IF EXISTS {table_z}; "
            f"CREATE TABLE {table_a} ("
            "id INT UNSIGNED NOT NULL PRIMARY KEY, "
            "value VARCHAR(64) NOT NULL"
            ") ENGINE=InnoDB; "
            f"CREATE TABLE {table_z} ("
            "id INT UNSIGNED NOT NULL PRIMARY KEY, "
            "value VARCHAR(64) NOT NULL"
            ") ENGINE=InnoDB;"
        )
        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, create_tables)

        started_values = ",".join(
            f"({index}, 'migration-a-{index}')" for index in range(1, 4001)
        )
        changed_values = "(1, 'migration-z-1'),(2, 'migration-z-2')"
        self.admin_sql(
            self.source,
            f"INSERT INTO {table_a} VALUES {started_values}; "
            f"INSERT INTO {table_z} VALUES {changed_values};",
        )
        self.admin_sql(self.target, f"INSERT INTO {table_z} VALUES {changed_values};")

        process, log_path = self.start_sync(
            tables=tables,
            run_id=run_id,
            chunk_size=10,
            parallelism=1,
        )
        expected_precondition = [
            f"prerequisite_schema\t{table_a}\tcomplete",
            f"prerequisite_schema\t{table_z}\tcomplete",
            f"rows\t{table_a}\trunning",
        ]
        deadline = time.monotonic() + 90
        progress_rows: list[str] = []
        try:
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    self.stop_sync_process(process)
                    raise HarnessError(
                        "authorized migration setup sync exited before interruption: "
                        f"{log_path.read_text()}"
                    )
                progress_rows = self.admin_query(
                    self.target,
                    "SELECT stage,table_name,status FROM cdc.sync_runs "
                    f"WHERE run_id={sql_literal(run_id)} "
                    "ORDER BY FIELD(stage,'prerequisite_schema','rows','final_constraints'),table_name;",
                ).splitlines()
                if progress_rows == expected_precondition:
                    break
                time.sleep(0.02)
            else:
                raise HarnessError(
                    "authorized migration setup did not reach the exact interrupted precondition: "
                    f"{progress_rows!r}"
                )
        finally:
            self.stop_sync_process(process)

        progress_rows = self.admin_query(
            self.target,
            "SELECT stage,table_name,status FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} "
            "ORDER BY FIELD(stage,'prerequisite_schema','rows','final_constraints'),table_name;",
        ).splitlines()
        if progress_rows != expected_precondition:
            raise HarnessError(
                "interrupted sync crossed the changed-table row boundary before termination: "
                f"{progress_rows!r}"
            )

        old_spec, old_sha256 = self.sync_run_spec_and_sha256(run_id)
        mismatched_specs = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} "
            f"AND BINARY run_spec_json <> BINARY {sql_literal(old_spec)};",
        ).strip()
        if mismatched_specs != "0":
            raise HarnessError(
                f"interrupted sync persisted multiple raw specifications: {mismatched_specs}"
            )
        before_wrong_hash = self.sync_progress_snapshot(run_id)
        prerequisite_before_migration = self.sync_progress_snapshot(
            run_id,
            stage="prerequisite_schema",
            include_run_spec=False,
        )
        preexisting_row_count = len(before_wrong_hash.splitlines())
        if preexisting_row_count != 3:
            raise HarnessError(
                f"unexpected preexisting migration row count: {preexisting_row_count}"
            )

        additive_columns = (
            " ADD COLUMN direct_seen_at TIMESTAMP(6) NULL,"
            " ADD COLUMN sync_seen_at TIMESTAMP(6) NULL"
        )
        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, f"ALTER TABLE {table_z}{additive_columns};")
        self.admin_sql(
            self.source,
            f"UPDATE {table_z} SET "
            "direct_seen_at='2026-08-21 12:00:00.123456', "
            "sync_seen_at='2026-08-21 12:05:00.654321';",
        )
        return (
            old_spec,
            old_sha256,
            before_wrong_hash,
            prerequisite_before_migration,
            preexisting_row_count,
        )

    def assert_wrong_hash_run_spec_migration_no_write(
        self,
        run_id: str,
        table_z: str,
        tables: list[str],
        before_wrong_hash: str,
    ) -> None:
        assert self.target
        wrong_hash = self.run_sync(
            tables=tables,
            run_id=run_id,
            chunk_size=10,
            parallelism=1,
            authorized_old_run_spec_sha256="0" * 64,
        )
        wrong_output = "\n".join((wrong_hash.stdout, wrong_hash.stderr))
        if wrong_hash.returncode == 0 or "does not match authorized SHA-256" not in wrong_output:
            raise HarnessError(
                f"wrong run-spec authorization did not fail at the hash boundary: {wrong_hash}"
            )
        if self.sync_progress_snapshot(run_id) != before_wrong_hash:
            raise HarnessError("wrong run-spec authorization changed durable progress")
        target_nulls = self.admin_query(
            self.target,
            f"SELECT COUNT(*) FROM {table_z} "
            "WHERE direct_seen_at IS NULL AND sync_seen_at IS NULL;",
        ).strip()
        if target_nulls != "2":
            raise HarnessError(
                f"wrong authorization changed target additive values: null_rows={target_nulls}"
            )

    def execute_successful_run_spec_migration(
        self,
        run_id: str,
        table_a: str,
        table_z: str,
        tables: list[str],
        old_spec: str,
        old_sha256: str,
        prerequisite_before_migration: str,
        preexisting_row_count: int,
    ) -> tuple[str, str]:
        assert self.source and self.target
        migrated = self.run_sync(
            tables=tables,
            run_id=run_id,
            chunk_size=10,
            parallelism=1,
            authorized_old_run_spec_sha256=old_sha256,
            timeout=240,
        )
        require_success(migrated, "authorized additive run-spec migration")
        migrated_audit = self.sync_run_spec_migration_audit(migrated)
        current_spec, current_sha256 = self.sync_run_spec_and_sha256(run_id)
        expected_migrated_audit = {
            "event": "sync_run_spec_migration",
            "run_id": run_id,
            "status": "migrated",
            "authorized_old_sha256": old_sha256,
            "old_sha256": old_sha256,
            "new_sha256": current_sha256,
            "locked_row_count": preexisting_row_count,
            "affected_row_count": preexisting_row_count,
            "delta": [
                {
                    "table": table_z,
                    "added_columns": ["direct_seen_at", "sync_seen_at"],
                }
            ],
        }
        if migrated_audit != expected_migrated_audit:
            raise HarnessError(
                f"unexpected migrated run-spec audit: {migrated_audit!r}"
            )
        if current_sha256 == old_sha256 or current_spec == old_spec:
            raise HarnessError("authorized additive migration retained the old run specification")

        terminal_rows = self.admin_query(
            self.target,
            "SELECT stage,table_name,status FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} "
            "ORDER BY FIELD(stage,'prerequisite_schema','rows','final_constraints'),table_name;",
        ).splitlines()
        expected_terminal_rows = [
            f"prerequisite_schema\t{table_a}\tcomplete",
            f"prerequisite_schema\t{table_z}\tcomplete",
            f"rows\t{table_a}\tcomplete",
            f"rows\t{table_z}\tcomplete",
            f"final_constraints\t{table_a}\tcomplete",
            f"final_constraints\t{table_z}\tcomplete",
        ]
        if terminal_rows != expected_terminal_rows:
            raise HarnessError(
                f"authorized migration did not produce six terminal rows: {terminal_rows!r}"
            )
        spec_counts = self.admin_query(
            self.target,
            "SELECT COUNT(*),"
            f"SUM(BINARY run_spec_json = BINARY {sql_literal(current_spec)}),"
            f"SUM(BINARY run_spec_json = BINARY {sql_literal(old_spec)}) "
            "FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)};",
        ).strip()
        if spec_counts != "6\t6\t0":
            raise HarnessError(f"terminal progress specification mismatch: {spec_counts!r}")
        prerequisite_after_migration = self.sync_progress_snapshot(
            run_id,
            stage="prerequisite_schema",
            include_run_spec=False,
        )
        if prerequisite_after_migration != prerequisite_before_migration:
            raise HarnessError(
                "authorized migration changed prerequisite progress outside run_spec_json"
            )

        table_a_state_sql = (
            f"SELECT COUNT(*),COALESCE(SUM(id),0),"
            f"COALESCE(SUM(CRC32(CONCAT(id,'|',value))),0) FROM {table_a};"
        )
        source_a_state = self.admin_query(self.source, table_a_state_sql).strip()
        target_a_state = self.admin_query(self.target, table_a_state_sql).strip()
        if source_a_state != target_a_state or not source_a_state.startswith("4000\t"):
            raise HarnessError(
                f"started table did not converge: source={source_a_state!r} target={target_a_state!r}"
            )
        table_z_state_sql = (
            f"SELECT id,value,DATE_FORMAT(direct_seen_at,'%Y-%m-%d %H:%i:%s.%f'),"
            f"DATE_FORMAT(sync_seen_at,'%Y-%m-%d %H:%i:%s.%f') FROM {table_z} ORDER BY id;"
        )
        source_z_state = self.admin_query(self.source, table_z_state_sql).strip()
        target_z_state = self.admin_query(self.target, table_z_state_sql).strip()
        if source_z_state != target_z_state:
            raise HarnessError(
                f"changed table additive values did not converge: source={source_z_state!r} target={target_z_state!r}"
            )
        return current_spec, current_sha256

    def assert_idempotent_run_spec_migration(
        self,
        run_id: str,
        tables: list[str],
        old_sha256: str,
        current_sha256: str,
    ) -> None:
        before_idempotent = self.sync_progress_snapshot(run_id)
        idempotent = self.run_sync(
            tables=tables,
            run_id=run_id,
            chunk_size=10,
            parallelism=1,
            authorized_old_run_spec_sha256=old_sha256,
            timeout=240,
        )
        require_success(idempotent, "idempotent authorized additive run-spec migration")
        idempotent_audit = self.sync_run_spec_migration_audit(idempotent)
        expected_idempotent_audit = {
            "event": "sync_run_spec_migration",
            "run_id": run_id,
            "status": "already_current",
            "authorized_old_sha256": old_sha256,
            "old_sha256": old_sha256,
            "new_sha256": current_sha256,
            "locked_row_count": 6,
            "affected_row_count": 0,
            "delta": [],
        }
        if idempotent_audit != expected_idempotent_audit:
            raise HarnessError(
                f"unexpected already-current run-spec audit: {idempotent_audit!r}"
            )
        if self.sync_progress_snapshot(run_id) != before_idempotent:
            raise HarnessError("idempotent authorized command changed durable progress")

    def assert_changed_table_row_progress_rejection(
        self,
        run_id: str,
        table_z: str,
        tables: list[str],
        current_sha256: str,
    ) -> None:
        assert self.source and self.target
        for endpoint in (self.source, self.target):
            self.admin_sql(
                endpoint,
                f"ALTER TABLE {table_z} "
                "ADD COLUMN third_seen_at TIMESTAMP(6) NULL;",
            )
        self.admin_sql(
            self.source,
            f"UPDATE {table_z} SET third_seen_at='2026-08-21 12:10:00.111222';",
        )
        before_rejection = self.sync_progress_snapshot(run_id)
        rejection = self.run_sync(
            tables=tables,
            run_id=run_id,
            chunk_size=10,
            parallelism=1,
            authorized_old_run_spec_sha256=current_sha256,
        )
        rejection_output = "\n".join((rejection.stdout, rejection.stderr))
        if (
            rejection.returncode == 0
            or f"changed table `{table_z}` already has rows-stage progress"
            not in rejection_output
        ):
            raise HarnessError(
                "changed-table row-progress gate did not reject the second migration: "
                f"{rejection}"
            )
        if self.sync_progress_snapshot(run_id) != before_rejection:
            raise HarnessError("changed-table row-progress rejection changed durable progress")
        third_target_nulls = self.admin_query(
            self.target,
            f"SELECT COUNT(*) FROM {table_z} WHERE third_seen_at IS NULL;",
        ).strip()
        if third_target_nulls != "2":
            raise HarnessError(
                "changed-table row-progress rejection changed target third-column values: "
                f"null_rows={third_target_nulls}"
            )

    def run_sync_resume(self) -> None:
        assert self.source and self.target
        self.setup_sync_accounts("sync_resume")
        values = ",".join(
            f"({index}, 'resume-{index}', 'source-{index}')"
            for index in range(1, 4001)
        )
        self.admin_sql(self.source, f"INSERT INTO sync_resume VALUES {values};")
        run_id = "sync-resume"
        process, log_path = self.start_sync(
            tables=["sync_resume"],
            run_id=run_id,
            chunk_size=10,
        )
        deadline = time.monotonic() + 90
        progress = ""
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise HarnessError(f"sync resume exited before interruption: {log_path.read_text()}")
            progress = self.admin_query(
                self.target,
                "SELECT status,chunks FROM cdc.sync_runs "
                "WHERE run_id='sync-resume' AND stage='rows' AND table_name='sync_resume';",
            ).strip()
            fields = progress.split("	")
            if len(fields) == 2 and fields[0] == "running" and int(fields[1]) >= 10:
                break
            time.sleep(0.1)
        else:
            raise HarnessError(f"sync resume did not persist interrupted progress: {progress}")
        process.kill()
        process.wait(timeout=30)
        log = getattr(process, "_cdc_log", None)
        if log is not None:
            log.close()

        changed = self.run_sync(
            tables=["sync_resume"],
            run_id=run_id,
            chunk_size=11,
        )
        changed_output = " ".join((changed.stdout, changed.stderr)).lower()
        if changed.returncode == 0 or "run specification mismatch" not in changed_output:
            raise HarnessError(f"sync resume accepted changed specification: {changed}")
        resumed = self.run_sync(
            tables=["sync_resume"],
            run_id=run_id,
            chunk_size=10,
            timeout=240,
        )
        require_success(resumed, "sync resume")
        count = self.query(
            self.target,
            "SELECT COUNT(*) FROM sync_resume;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if count != "4000":
            raise HarnessError(f"sync resume did not converge: {count}")
        print("sync_resume_ok same_run_id=true changed_spec_rejected=true rows=4000")

    def run_writable_column_generated_metadata(self) -> None:
        assert self.source and self.target
        for endpoint in (self.source, self.target):
            self.admin_sql(
                endpoint,
                "DROP TABLE IF EXISTS writable_column_metadata; "
                "CREATE TABLE writable_column_metadata ("
                "id INT NOT NULL PRIMARY KEY, "
                "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                "payload INT NOT NULL, "
                "virtual_value INT GENERATED ALWAYS AS (payload + 1) VIRTUAL, "
                "stored_value INT GENERATED ALWAYS AS (payload + 2) STORED"
                ") ENGINE=InnoDB;",
            )
        legacy_query = (
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='globalcomix' "
            "AND table_name='writable_column_metadata' "
            "AND extra NOT LIKE '%GENERATED%' ORDER BY ordinal_position;"
        )
        writable_query = (
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='globalcomix' "
            "AND table_name='writable_column_metadata' "
            "AND UPPER(extra) NOT LIKE '%VIRTUAL GENERATED%' "
            "AND UPPER(extra) NOT LIKE '%STORED GENERATED%' "
            "ORDER BY ordinal_position;"
        )
        source_columns = self.admin_query(self.source, writable_query).splitlines()
        target_columns = self.admin_query(self.target, writable_query).splitlines()
        legacy_target = self.admin_query(self.target, legacy_query).splitlines()
        expected = ["id", "created_at", "payload"]
        if source_columns != expected or target_columns != expected:
            raise HarnessError(
                "writable metadata classification mismatch: "
                f"source={source_columns!r} target={target_columns!r}"
            )
        if "created_at" in legacy_target:
            raise HarnessError(
                "MySQL DEFAULT_GENERATED did not reproduce legacy exclusion"
            )
        print(
            "writable_column_generated_metadata_ok default_generated=writable "
            "virtual_generated=excluded stored_generated=excluded "
            "source_target_equal=true legacy_target_excludes_default=true"
        )

    def conflict_identity(
        self,
        source_file: str,
        start_position: int,
        table: str,
        primary_key: list[str],
        operation: str = "update",
    ) -> str:
        import hashlib
        import struct

        fields = [
            SOURCE_IDENTITY.encode(),
            struct.pack(">Q", 101),
            source_file.encode(),
            struct.pack(">Q", start_position),
            APP_SCHEMA.encode(),
            table.encode(),
            operation.encode(),
            json.dumps(primary_key, separators=(",", ":")).encode(),
        ]
        encoded = b"".join(struct.pack(">Q", len(field)) + field for field in fields)
        return hashlib.sha256(encoded).hexdigest()

    def source_row_identity(self, table: str, primary_key: list[str]) -> str:
        import hashlib
        import struct

        fields = [
            SOURCE_IDENTITY.encode(),
            APP_SCHEMA.encode(),
            table.encode(),
            json.dumps(primary_key, separators=(",", ":")).encode(),
        ]
        encoded = b"".join(struct.pack(">Q", len(field)) + field for field in fields)
        return hashlib.sha256(encoded).hexdigest()

    def run_bootstrap_contract(self) -> None:
        assert self.target
        self._assert_target_grants()
        for table in ("stream_checkpoint", "row_conflicts", "ddl_replay_journal"):
            count = self.admin_query(
                self.target,
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='cdc' AND table_name={sql_literal(table)};",
            ).strip()
            if count != "1":
                raise HarnessError(f"bootstrap missing cdc.{table}")
        routine_count = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM information_schema.routines "
            "WHERE routine_schema='cdc' AND routine_name='row_conflicts_trigger_inventory';",
        ).strip()
        if routine_count != "1":
            raise HarnessError("bootstrap missing cdc.row_conflicts_trigger_inventory")
        print("bootstrap_contract_ok grants=exact schemas=routines_present")

    def run_startup_rejection(self, scenario: str) -> None:
        assert self.source and self.target
        self.setup_accounts_table()
        start = self.coordinate()
        self.write_checkpoint(start)
        if scenario == "missing-checkpoint":
            self.admin_sql(self.target, "DELETE FROM cdc.stream_checkpoint;")
            expected = "checkpoint"
        elif scenario == "missing-trigger":
            self.admin_sql(self.target, "DROP TRIGGER cdc.ddl_replay_journal_update_guard;")
            expected = "trigger"
        elif scenario == "missing-grant":
            self.admin_sql(self.target, "REVOKE UPDATE ON cdc.ddl_replay_journal FROM 'cdc_stream'@'%';")
            expected = "grant"
        elif scenario == "journal-outage":
            self.admin_sql(self.target, "RENAME TABLE cdc.ddl_replay_journal TO cdc.ddl_replay_journal_outage;")
            expected = "journal"
        else:
            raise HarnessError(f"unknown startup rejection scenario: {scenario}")
        result = self.run_stream(start)
        if result.returncode == 0 or expected not in (result.stderr + result.stdout).lower():
            raise HarnessError(
                f"{scenario} did not fail at the expected startup boundary:\n"
                f"stdout={result.stdout}\nstderr={result.stderr}"
            )
        rows = self.admin_query(self.target, "SELECT COUNT(*) FROM globalcomix.accounts;").strip()
        if rows != "0":
            raise HarnessError(f"{scenario} mutated target before startup rejection: {rows}")
        print(f"{scenario}_rejected boundary={expected}")

    def run_translation_pending_barrier(self) -> None:
        assert self.source and self.target
        self.setup_accounts_table()
        start = self.coordinate()
        self.write_checkpoint(start)
        self.admin_sql(
            self.source,
            "CREATE UNIQUE INDEX idx_accounts_email_unique ON accounts (email);",
        )
        stop = self.coordinate()
        result = self.run_stream(start, stop)
        combined = (result.stdout + result.stderr).lower()
        if result.returncode == 0 or "translator unavailable" not in combined:
            raise HarnessError(
                "unsupported DDL did not stop at the translation barrier:\n"
                f"stdout={result.stdout}\nstderr={result.stderr}"
            )
        target_indexes = self.query(
            self.target,
            "SHOW INDEX FROM accounts;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        )
        if "idx_accounts_email_unique" in target_indexes:
            raise HarnessError("translation-pending DDL mutated target")
        rows = self.query(
            self.target,
            "SELECT status,transformation_version,generated_sql,canonical_ast,pre_state,expected_post_state "
            "FROM cdc.ddl_replay_journal ORDER BY event_start_position;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).splitlines()
        if rows != ["translation_pending\ttranslator-unavailable\tNULL\t\t\t"]:
            raise HarnessError(f"unexpected translation-pending journal evidence: {rows}")
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != start.file or int(checkpoint.get("source_position", 0)) != start.position:
            raise HarnessError(f"translation-pending DDL advanced checkpoint: {checkpoint}")
        print(f"translation_pending_barrier_ok coordinate={start.file}:{start.position} rows=1")

    def run_scenario(self, scenario: str) -> None:
        spec = SCENARIO_BY_NAME[scenario]
        if not spec.executable:
            raise HarnessSkip(spec.prerequisite)
        self.prepare()
        if scenario == "strict-secondary-btree":
            self.run_strict_secondary_btree()
        elif scenario == "sync-tls":
            self.run_sync_tls()
        elif scenario == "sync-composite-enum-primary-key":
            self.run_sync_composite_enum_primary_key()
        elif scenario == "sync-fk-parent-insert":
            self.run_sync_fk_parent_convergence()
        elif scenario == "sync-fk-parent-update":
            self.run_sync_fk_parent_convergence(update_existing_child=True)
        elif scenario == "sync-fk-parent-stale-unique-owner":
            self.run_sync_fk_parent_stale_unique_owner()
        elif scenario == "sync-wide-update":
            self.run_sync_wide_update()
        elif scenario == "sync-resume":
            self.run_sync_resume()
        elif scenario == "sync-authorized-additive-spec-migration":
            self.run_sync_authorized_additive_spec_migration()
        elif scenario == "sync-progress-least-privilege":
            self.run_sync_progress_least_privilege()
        elif scenario == "writable-column-generated-metadata":
            self.run_writable_column_generated_metadata()
        elif scenario == "production-alter-table":
            self.run_production_alter_table()
        elif scenario == "create-table-crash-restart":
            self.run_create_table_crash_restart()
        elif scenario == "bootstrap-contract":
            self.run_bootstrap_contract()
        elif scenario == "insert-duplicate-idempotent":
            self.run_insert_duplicate_idempotent()
        elif scenario == "missing-fk-parent-auto-insert":
            self.run_missing_fk_parent_auto_insert()
        elif scenario == "missing-fk-nested-parent-auto-insert":
            self.run_missing_fk_nested_parent_auto_insert()
        elif scenario == "missing-fk-superseded-insert":
            self.run_missing_fk_superseded_insert()
        elif scenario == "missing-fk-duplicate-parent-reconcile":
            self.run_missing_fk_duplicate_parent_reconcile()
        elif scenario in {
            "missing-checkpoint",
            "missing-trigger",
            "missing-grant",
            "journal-outage",
        }:
            self.run_startup_rejection(scenario)
        elif scenario == "translation-pending-barrier":
            self.run_translation_pending_barrier()
        elif scenario in {
            "prepare-failure",
            "post-ddl-pre-applied",
            "applied-pre-checkpoint",
            "checkpoint-transaction",
        }:
            self.run_recovery_scenario(scenario)
        elif scenario in {"source-connection-loss", "target-connection-loss"}:
            self.run_connection_loss_scenario(scenario)
        elif scenario == "row-conflict-source-row-migration":
            self.run_row_conflict_source_row_migration()
        elif scenario in {
            "pre-state-drift",
            "coordinate-reuse",
            "raw-sql-reuse",
            "end-position-reuse",
            "checkpoint-mismatch",
        }:
            self.run_journal_mismatch_scenario(scenario)
        else:
            raise HarnessError(f"scenario has no implementation: {scenario}")


def make_tls_material_container_readable(tempdir: Path, files: Iterable[Path]) -> None:
    tempdir.chmod(0o755)
    for path in files:
        path.chmod(0o644)


def container_logs(container: str) -> str:
    result = run(["docker", "logs", container], check=False)
    output = "\n".join(part for part in (result.stdout.strip(), result.stderr.strip()) if part)
    return output or "<no container logs>"


def wait_for_sql(endpoint: Endpoint, ca_file: Path, timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        result = run(
            [
                "mariadb",
                "--protocol=tcp",
                "--ssl",
                f"--ssl-ca={ca_file}",
                "--ssl-verify-server-cert",
                "--host=127.0.0.1",
                f"--port={endpoint.port}",
                "--user=root",
                f"--password={ADMIN_PASSWORD}",
                "--batch",
                "--skip-column-names",
                "-e",
                "SELECT 1",
            ],
            check=False,
        )
        if result.returncode == 0:
            return
        last_error = result.stderr.strip()
        time.sleep(1)
    raise HarnessError(
        f"database did not become ready endpoint={endpoint}: {last_error}\n"
        f"container_logs:\n{container_logs(endpoint.container)}"
    )


def normalize_grants(output: str) -> list[str]:
    normalized = []
    for line in output.splitlines():
        line = line.strip().replace("`", "")
        if not line:
            continue
        line = re.sub(r"\s+", " ", line)
        normalized.append(line)
    return normalized


def canonicalize_privileges(privileges: frozenset[str]) -> frozenset[str]:
    return frozenset(
        "REPLICATION CLIENT" if privilege == "BINLOG MONITOR" else privilege
        for privilege in privileges
    )


def discard_implicit_usage(
    grants: set[tuple[frozenset[str], str]],
) -> set[tuple[frozenset[str], str]]:
    if any(privileges != frozenset({"USAGE"}) for privileges, _scope in grants):
        return {grant for grant in grants if grant[0] != frozenset({"USAGE"})}
    return grants


def assert_exact_grants(
    grants: list[str],
    expected: set[tuple[frozenset[str], str]],
    user: str,
) -> None:
    actual: set[tuple[frozenset[str], str]] = set()
    for grant in grants:
        if "WITH GRANT OPTION" in grant.upper() or " PROXY " in grant.upper() or "GRANT ROLE" in grant.upper():
            raise HarnessError(f"unsafe effective grant for {user}: {grant}")
        match = re.match(r"GRANT (.+?) ON (.+?) TO ", grant, flags=re.IGNORECASE)
        if not match:
            raise HarnessError(f"unparseable effective grant for {user}: {grant}")
        privileges = canonicalize_privileges(
            frozenset(part.strip().upper() for part in match.group(1).split(","))
        )
        scope = match.group(2).strip().lower()
        actual.add((privileges, scope))
    normalized_expected = {
        (canonicalize_privileges(privileges), scope.lower()) for privileges, scope in expected
    }
    if any(privileges != frozenset({"USAGE"}) for privileges, _scope in actual | normalized_expected):
        actual = discard_implicit_usage(actual)
        normalized_expected = discard_implicit_usage(normalized_expected)
    if actual != normalized_expected:
        raise HarnessError(
            f"effective grants mismatch for {user}: expected={sorted(normalized_expected, key=str)} actual={sorted(actual, key=str)}"
        )


def sql_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def coordinate_is_after(left: Coordinate, right: Coordinate) -> bool:
    return left.file > right.file or (left.file == right.file and left.position > right.position)


def require_success(result: CommandResult, operation: str) -> None:
    if result.returncode:
        raise HarnessError(
            f"{operation} failed (exit {result.returncode}):\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def require_translation_pending_termination(result: CommandResult) -> None:
    output = f"{result.stdout}\n{result.stderr}".lower()
    if result.returncode == 0:
        raise HarnessError("bounded stream returned success without translation-pending block")
    if "translator unavailable" not in output:
        raise HarnessError(
            "unsupported DDL did not terminate at the translation-pending boundary:\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )


def require_command(name: str) -> None:
    if shutil.which(name) is None:
        raise HarnessSkip(f"required command missing: {name}")


def run(
    command: Iterable[str],
    *,
    input_text: str | None = None,
    env: dict[str, str] | None = None,
    timeout: float | None = 120,
    check: bool = True,
    cwd: Path | None = None,
) -> CommandResult:
    argv = tuple(str(part) for part in command)
    try:
        completed = subprocess.run(
            argv,
            input=input_text,
            text=True,
            capture_output=True,
            env=env,
            timeout=timeout,
            check=False,
            cwd=cwd,
        )
    except subprocess.TimeoutExpired as error:
        raise HarnessError(f"command timed out: {' '.join(argv)}") from error
    result = CommandResult(argv, completed.returncode, completed.stdout, completed.stderr)
    if check and result.returncode:
        raise HarnessError(
            f"command failed ({result.returncode}): {' '.join(argv)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="list scenarios with executable/prerequisite status")
    parser.add_argument("--scenario", action="append", choices=tuple(SCENARIO_BY_NAME), help="run one scenario")
    parser.add_argument("--binary", type=Path, help="path to the built mariadb-mysql-cdc binary")
    parser.add_argument("--keep", action="store_true", help="keep temporary containers/files for diagnosis")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.list:
        for scenario in SCENARIOS:
            if scenario.executable:
                print(f"{scenario.name}\texecutable")
            else:
                print(f"{scenario.name}\tskipped\t{scenario.prerequisite}")
        return 0
    scenarios = args.scenario or default_scenarios()
    repo = Path(__file__).resolve().parents[1]
    try:
        for scenario in scenarios:
            print(f"scenario_start name={scenario}")
            with Harness(repo, args.binary, args.keep) as harness:
                harness.run_scenario(scenario)
            print(f"scenario_pass name={scenario}")
    except HarnessSkip as skip:
        print(f"harness_skip prerequisite={skip}")
        return 0
    except HarnessError as error:
        print(f"harness_error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
