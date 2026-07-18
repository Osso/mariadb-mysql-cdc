#!/usr/bin/env python3
"""Disposable MariaDB -> MySQL CDC proof harness.

The executable scenarios use dedicated least-privilege accounts and strict TLS.
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
TARGET_USER = "cdc_stream"
TARGET_PASSWORD = "cdc-stream-password"
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
    ScenarioSpec("production-alter-table", True),
    ScenarioSpec("create-table-crash-restart", True),
    ScenarioSpec("bootstrap-contract", True),
    ScenarioSpec("catchup-snapshot-tls", True),
    ScenarioSpec("missing-checkpoint", True),
    ScenarioSpec("missing-trigger", True),
    ScenarioSpec("missing-conflict-trigger", True),
    ScenarioSpec("missing-grant", True),
    ScenarioSpec("missing-conflict-table", True),
    ScenarioSpec("wrong-conflict-schema", True),
    ScenarioSpec("missing-conflict-grant", True),
    ScenarioSpec("broad-conflict-grant", True),
    ScenarioSpec("journal-outage", True),
    ScenarioSpec("translation-pending-barrier", True),
    ScenarioSpec("prepare-failure", True),
    ScenarioSpec("post-ddl-pre-applied", True),
    ScenarioSpec("applied-pre-checkpoint", True),
    ScenarioSpec("checkpoint-transaction", True),
    ScenarioSpec("source-connection-loss", True),
    ScenarioSpec("target-connection-loss", True),
    ScenarioSpec("replace-divergent-pk", True),
    ScenarioSpec("row-conflict-rollback", True),
    ScenarioSpec("pre-state-drift", True),
    ScenarioSpec("coordinate-reuse", True),
    ScenarioSpec("raw-sql-reuse", True),
    ScenarioSpec("end-position-reuse", True),
    ScenarioSpec("checkpoint-mismatch", True),
    ScenarioSpec("fk-child-first-delete", True),
    ScenarioSpec("fk-parent-first-insert", True),
    ScenarioSpec("fk-cycle-block", True),
    ScenarioSpec("repair-resume", True),
    ScenarioSpec("bounded-delete", True),
    ScenarioSpec("conflict-resolution-zero-debt", True),
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
        self._assert_endpoint_tls(self.target, TARGET_USER, TARGET_PASSWORD, "target")
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
        grants = self.admin_query(self.target, "SHOW GRANTS FOR 'cdc_stream'@'%';")
        print("cdc_stream_show_grants_begin")
        for row in grants.splitlines():
            print(f"cdc_stream_show_grant row={row}")
        print("cdc_stream_show_grants_end")
        normalized = normalize_grants(grants)
        assert_exact_grants(
            normalized,
            {
                (frozenset({"USAGE"}), "*.*"),
                (
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
                ),
                (frozenset({"SELECT", "INSERT", "UPDATE"}), "cdc.stream_checkpoint"),
                (frozenset({"SELECT", "INSERT", "UPDATE"}), "cdc.row_conflicts"),
                (frozenset({"EXECUTE"}), "PROCEDURE cdc.row_conflicts_trigger_inventory"),
                (frozenset({"SELECT", "INSERT", "UPDATE"}), "cdc.ddl_replay_journal"),
                (frozenset({"EXECUTE"}), "PROCEDURE cdc.ddl_replay_journal_trigger_inventory"),
            },
            TARGET_USER,
        )

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
        insert_conflict_policy: str | None = None,
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
            TARGET_USER,
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
        if insert_conflict_policy is not None:
            args.extend(["--insert-conflict-policy", insert_conflict_policy])
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
        insert_conflict_policy: str | None = None,
    ) -> CommandResult:
        binary = self._stream_binary(integration_failpoint)
        env = {**os.environ, "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD, "CDC_TARGET_PASSWORD": TARGET_PASSWORD}
        if barrier_dir is not None:
            env["CDC_INTEGRATION_BARRIER_DIR"] = str(barrier_dir)
        return run(
            self._stream_args(
                binary,
                start,
                stop,
                integration_failpoint,
                max_reconnects,
                insert_conflict_policy,
            ),
            env=env,
            timeout=90,
            check=False,
        )

    def _catchup_args(
        self,
        binary: Path,
        progress_file: Path,
        *,
        target_ca_file: Path | None = None,
    ) -> list[str]:
        assert self.source and self.target
        target_ca_file = target_ca_file or self.ca_file
        return [
            str(binary),
            "catchup-snapshot",
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
            TARGET_USER,
            "--target-password-env",
            "CDC_TARGET_PASSWORD",
            "--target-database",
            APP_SCHEMA,
            "--target-tls-ca-file",
            str(target_ca_file),
            "--progress-file",
            str(progress_file),
            "--progress-table",
            f"{APP_SCHEMA}.table_sync_progress",
            "--chunk-size",
            "2",
            "--parallel-workers",
            "2",
            "--table",
            "accounts",
        ]

    def _sync_table_args(
        self,
        binary: Path,
    ) -> list[str]:
        assert self.source and self.target
        return [
            str(binary),
            "sync-table",
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
            TARGET_USER,
            "--target-password-env",
            "CDC_TARGET_PASSWORD",
            "--target-database",
            APP_SCHEMA,
            "--target-tls-ca-file",
            str(self.ca_file),
            "--table",
            "accounts",
            "--primary-key",
            "id",
            "--columns",
            "id,email,payload",
            "--run-id",
            "sync-table-source-ca-proof",
            "--progress-table",
            "globalcomix.sync_table_tls_progress",
        ]

    def _repair_binary(self) -> Path:
        binary = self.binary or self.repo / "target/debug/mariadb-mysql-cdc"
        if not binary.is_file():
            run(["cargo", "build", "--bin", "mariadb-mysql-cdc"], cwd=self.repo)
        if not binary.is_file():
            raise HarnessError(f"CDC binary build did not produce {binary}")
        return binary

    def _repair_args(
        self,
        binary: Path,
        *,
        tables: list[str],
        mode: str,
        max_deletes: int,
        run_id: str | None = None,
        chunk_size: int = 1000,
        start_after: list[str] | None = None,
        end_at: list[str] | None = None,
        progress_table: str = "globalcomix.table_sync_runs",
    ) -> list[str]:
        args = [
            str(binary),
            "repair-drift",
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
            "--target-host",
            "127.0.0.1",
            "--target-port",
            str(self.target.port),
            "--target-user",
            TARGET_USER,
            "--target-password-env",
            "CDC_TARGET_PASSWORD",
            "--target-database",
            APP_SCHEMA,
            "--target-tls-ca-file",
            str(self.ca_file),
            "--mode",
            mode,
            "--max-deletes",
            str(max_deletes),
            "--chunk-size",
            str(chunk_size),
            "--progress-table",
            progress_table,
        ]
        for table in tables:
            args.extend(["--table", table])
        if run_id is not None:
            args.extend(["--run-id", run_id])
        if start_after is not None:
            args.extend(["--start-after-json", json.dumps(start_after)])
        if end_at is not None:
            args.extend(["--end-at-json", json.dumps(end_at)])
        return args

    def run_repair(
        self,
        *,
        tables: list[str],
        mode: str = "apply",
        max_deletes: int = 0,
        run_id: str | None = None,
        chunk_size: int = 1000,
        start_after: list[str] | None = None,
        end_at: list[str] | None = None,
        progress_table: str = "globalcomix.table_sync_runs",
        timeout: float = 180,
    ) -> CommandResult:
        assert self.source and self.target
        binary = self._repair_binary()
        env = {
            **os.environ,
            "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD,
            "CDC_TARGET_PASSWORD": TARGET_PASSWORD,
        }
        return run(
            self._repair_args(
                binary,
                tables=tables,
                mode=mode,
                max_deletes=max_deletes,
                run_id=run_id,
                chunk_size=chunk_size,
                start_after=start_after,
                end_at=end_at,
                progress_table=progress_table,
            ),
            cwd=self.repo,
            env=env,
            timeout=timeout,
            check=False,
        )

    def start_repair(
        self,
        *,
        tables: list[str],
        max_deletes: int,
        run_id: str,
        chunk_size: int,
        progress_table: str = "globalcomix.table_sync_runs",
    ) -> tuple[subprocess.Popen[str], Path]:
        assert self.source and self.target
        binary = self._repair_binary()
        env = {
            **os.environ,
            "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD,
            "CDC_TARGET_PASSWORD": TARGET_PASSWORD,
        }
        log_path = self.tempdir / f"{run_id}.log"
        log = log_path.open("w")
        process = subprocess.Popen(
            self._repair_args(
                binary,
                tables=tables,
                mode="apply",
                max_deletes=max_deletes,
                run_id=run_id,
                chunk_size=chunk_size,
                progress_table=progress_table,
            ),
            cwd=self.repo,
            env=env,
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
        env = {**os.environ, "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD, "CDC_TARGET_PASSWORD": TARGET_PASSWORD}
        if barrier_dir is not None:
            env["CDC_INTEGRATION_BARRIER_DIR"] = str(barrier_dir)
        log_path = self.tempdir / f"{label}.log"
        log = log_path.open("w")
        process = subprocess.Popen(
            self._stream_args(binary, start, stop, integration_failpoint, max_reconnects),
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

    def _assert_catchup_target_unchanged(self) -> None:
        assert self.target
        row_count = self.admin_query(self.target, "SELECT COUNT(*) FROM accounts;").strip()
        progress_table_count = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='globalcomix' "
            "AND table_name='table_sync_progress';",
        ).strip()
        if row_count != "0" or progress_table_count != "0":
            raise HarnessError(
                "rejected catchup mutated target: "
                f"rows={row_count!r} progress_tables={progress_table_count!r}"
            )

    def _assert_catchup_ca_rejected(
        self,
        binary: Path,
        progress_file: Path,
        env: dict[str, str],
        *,
        target_ca_file: Path,
        label: str,
    ) -> None:
        result = run(
            self._catchup_args(
                binary,
                progress_file,
                target_ca_file=target_ca_file,
            ),
            env=env,
            timeout=90,
            check=False,
        )
        if result.returncode == 0:
            raise HarnessError(f"catchup accepted {label}")
        diagnostic = f"{result.stdout}\n{result.stderr}".lower()
        if not any(marker in diagnostic for marker in ("certificate", "ssl", "tls")):
            raise HarnessError(
                f"catchup {label} lacked TLS diagnostic: {diagnostic!r}"
            )
        self._assert_catchup_target_unchanged()

    def _assert_sync_table_source_ca_rejected(
        self,
        binary: Path,
        env: dict[str, str],
    ) -> None:
        result = run(
            self._sync_table_args(
                binary,
            ),
            env=env,
            timeout=90,
            check=False,
        )
        if result.returncode == 0:
            raise HarnessError("sync-table accepted untrusted source CA")
        diagnostic = f"{result.stdout}\n{result.stderr}".lower()
        if not any(marker in diagnostic for marker in ("certificate", "ssl", "tls")):
            raise HarnessError(
                f"sync-table wrong source CA lacked TLS diagnostic: {diagnostic!r}"
            )

    def run_catchup_snapshot_tls(self) -> None:
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
        binary = self._repair_binary()
        progress_file = self.tempdir / "catchup-snapshot-tls-progress.json"
        args = self._catchup_args(binary, progress_file)
        env = {
            **os.environ,
            "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD,
            "CDC_TARGET_PASSWORD": TARGET_PASSWORD,
        }

        self._assert_sync_table_source_ca_rejected(binary, env)
        self._assert_catchup_ca_rejected(
            binary,
            progress_file,
            env,
            target_ca_file=self.ca_file,
            label="untrusted source CA",
        )
        self._assert_catchup_ca_rejected(
            binary,
            progress_file,
            env,
            target_ca_file=self.unrelated_ca_file,
            label="untrusted target CA",
        )

        first = run(args, env=env, timeout=90, check=False)
        require_success(first, "catchup snapshot TLS")
        expected_rows = (
            "1\tone@example.test\tone\n"
            "2\ttwo@example.test\ttwo\n"
            "3\tthree@example.test\tthree\n"
            "4\tfour@example.test\tfour"
        )
        copied_rows = self.query(
            self.target,
            "SELECT id,email,payload FROM accounts ORDER BY id;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if copied_rows != expected_rows:
            raise HarnessError(f"catchup TLS copied rows mismatch: {copied_rows!r}")
        progress_row = self.query(
            self.target,
            "SELECT table_name,status,rows_scanned FROM globalcomix.table_sync_progress "
            "WHERE table_name='accounts';",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if progress_row != "accounts\tcomplete\t4":
            raise HarnessError(f"catchup TLS progress row mismatch: {progress_row!r}")

        self.admin_sql(
            self.target,
            "SET GLOBAL general_log=OFF; TRUNCATE TABLE mysql.general_log; "
            "SET GLOBAL log_output='TABLE'; SET GLOBAL general_log=ON;",
        )
        replay = run(args, env=env, timeout=90, check=False)
        account_insert_attempts = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM mysql.general_log WHERE user_host LIKE 'cdc_stream%' "
            "AND command_type IN ('Query','Prepare','Execute') "
            "AND UPPER(argument) LIKE 'INSERT%ACCOUNTS%';",
        ).strip()
        self.admin_sql(self.target, "SET GLOBAL general_log=OFF;")
        require_success(replay, "catchup snapshot TLS completed rerun")
        if account_insert_attempts != "0":
            raise HarnessError(
                "catchup TLS completed rerun attempted account inserts: "
                f"{account_insert_attempts}"
            )
        replayed_rows = self.query(
            self.target,
            "SELECT id,email,payload FROM accounts ORDER BY id;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if replayed_rows != expected_rows:
            raise HarnessError(
                f"catchup TLS completed rerun changed rows: {replayed_rows!r}"
            )
        print(
            "catchup_snapshot_tls_converged rows=4 source_ca=true target_ca=true "
            "sync_table_wrong_source_ca_rejected=true "
            "wrong_source_ca_rejected=true wrong_target_ca_rejected=true "
            "parallel_workers=2 completed_rerun_noop=true"
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

    def run_replace_divergent_pk(self) -> None:
        assert self.source and self.target
        self.setup_accounts_table()
        self.admin_sql(
            self.target,
            "INSERT INTO accounts VALUES (1, 'target@example.test', 'target');",
        )
        start = self.coordinate()
        self.write_checkpoint(start)
        self.admin_sql(
            self.source,
            "INSERT INTO accounts VALUES (1, 'source@example.test', 'source');",
        )
        stop = self.coordinate()

        result = self.run_stream(
            start,
            stop,
            insert_conflict_policy="replace-divergent-pk",
        )
        output = f"{result.stdout}\n{result.stderr}".lower()
        require_success(result, "replace-divergent-pk commit")
        if "cdc_row_conflict_replaced" not in output or 'primary_key=["1"]' not in output:
            raise HarnessError(f"replacement did not report durable row evidence: {output}")
        row = self.admin_query(self.target, "SELECT email,payload FROM accounts WHERE id=1;").strip()
        if row != "source@example.test\tsource":
            raise HarnessError(f"replacement did not install source image: {row!r}")
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != stop.file or int(checkpoint.get("source_position", 0)) != stop.position:
            raise HarnessError(f"replacement did not commit checkpoint at XID end: {checkpoint}")
        evidence = self.admin_query(
            self.target,
            "SELECT source_primary_key_json,error_code,attempt_count,status FROM cdc.row_conflicts "
            "WHERE table_name='accounts' ORDER BY conflict_identity;",
        ).strip()
        if evidence != '["1"]\t1062\t1\tunresolved':
            raise HarnessError(f"replacement evidence mismatch after commit: {evidence!r}")

        self.admin_sql(
            self.target,
            "UPDATE accounts SET email='target@example.test', payload='target' WHERE id=1;",
        )
        self.write_checkpoint(start)
        replay = self.run_stream(
            start,
            stop,
            insert_conflict_policy="replace-divergent-pk",
        )
        replay_output = f"{replay.stdout}\n{replay.stderr}".lower()
        require_success(replay, "replace-divergent-pk replay")
        if "cdc_row_conflict_replaced" not in replay_output:
            raise HarnessError(f"replacement replay did not report evidence: {replay_output}")
        evidence = self.admin_query(
            self.target,
            "SELECT source_primary_key_json,error_code,attempt_count,status FROM cdc.row_conflicts "
            "WHERE table_name='accounts' ORDER BY conflict_identity;",
        ).strip()
        if evidence != '["1"]\t1062\t2\tunresolved':
            raise HarnessError(f"replacement evidence was not idempotently updated: {evidence!r}")

        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, "DROP TABLE IF EXISTS replace_failure_rows;")
        self.admin_sql(
            self.source,
            "CREATE TABLE replace_failure_rows (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL) ENGINE=InnoDB;",
        )
        self.admin_sql(
            self.target,
            "CREATE TABLE replace_failure_rows (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL, "
            "CONSTRAINT chk_replace_failure_rows CHECK (payload <> 'blocked')) ENGINE=InnoDB;",
        )
        self.admin_sql(self.target, "INSERT INTO replace_failure_rows VALUES (1, 'target');")
        failure_start = self.coordinate()
        self.write_checkpoint(failure_start)
        self.admin_sql(self.source, "INSERT INTO replace_failure_rows VALUES (1, 'blocked');")
        failure_stop = self.coordinate()
        for attempt in (1, 2):
            failure = self.run_stream(
                failure_start,
                failure_stop,
                insert_conflict_policy="replace-divergent-pk",
            )
            failure_output = f"{failure.stdout}\n{failure.stderr}".lower()
            if failure.returncode == 0 or "row conflict persisted for repair" not in failure_output:
                raise HarnessError(f"replacement update failure attempt {attempt} did not abort: {failure_output}")
            row = self.admin_query(self.target, "SELECT payload FROM replace_failure_rows WHERE id=1;").strip()
            if row != "target":
                raise HarnessError(f"replacement update failure retained target mutation: {row!r}")
            checkpoint = self.checkpoint()
            if checkpoint.get("source_file") != failure_start.file or int(checkpoint.get("source_position", 0)) != failure_start.position:
                raise HarnessError(f"replacement update failure advanced checkpoint: {checkpoint}")
            evidence = self.admin_query(
                self.target,
                "SELECT source_primary_key_json,error_code,attempt_count,status FROM cdc.row_conflicts "
                "WHERE table_name='replace_failure_rows' ORDER BY conflict_identity;",
            ).strip()
            expected = f'["1"]\t3819\t{attempt}\tunresolved'
            if evidence != expected and not evidence.startswith(f'["1"]\t4025\t{attempt}\tunresolved'):
                raise HarnessError(f"replacement update failure evidence mismatch: {evidence!r}")
        print(
            "replace_divergent_pk_ok xid_commit_checkpoint=true replacement_attempts=2 "
            "update_failure_rollback=true update_failure_attempts=2 crash_boundary_proven=false"
        )

    def run_row_conflict_rollback(self) -> None:
        assert self.source and self.target
        self.setup_accounts_table()
        equal_row = "INSERT INTO accounts VALUES (1, 'same@example.test', 'same');"
        self.admin_sql(self.target, equal_row)
        equal_start = self.coordinate()
        self.write_checkpoint(equal_start)
        self.admin_sql(self.source, equal_row)
        equal_stop = self.coordinate()
        equal_result = self.run_stream(
            equal_start,
            equal_stop,
            insert_conflict_policy="ignore-duplicate",
        )
        equal_output = f"{equal_result.stdout}\n{equal_result.stderr}".lower()
        if (
            equal_result.returncode != 0
            or "cdc_row_conflict_skipped" not in equal_output
            or 'primary_key=["1"]' not in equal_output
        ):
            raise HarnessError(f"equal-PK duplicate did not continue cleanly: {equal_output}")
        rows = self.admin_query(
            self.target,
            "SELECT id,email,payload FROM accounts ORDER BY id;",
        ).strip()
        if rows != "1\tsame@example.test\tsame":
            raise HarnessError(f"equal-PK duplicate changed the target row: {rows!r}")
        checkpoint = self.checkpoint()
        if (
            checkpoint.get("source_file") != equal_stop.file
            or int(checkpoint.get("source_position", 0)) != equal_stop.position
        ):
            raise HarnessError(f"equal-PK duplicate did not advance checkpoint: {checkpoint}")
        unresolved = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM cdc.row_conflicts "
            "WHERE table_name='accounts' AND source_primary_key_json=JSON_ARRAY('1') "
            "AND status='unresolved';",
        ).strip()
        if unresolved != "0":
            raise HarnessError(f"equal-PK duplicate created unresolved conflict debt: {unresolved}")
        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, "DELETE FROM accounts WHERE id=1;")
        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, "ALTER TABLE accounts ADD UNIQUE KEY uq_accounts_email (email);")
        self.admin_sql(self.target, "INSERT INTO accounts VALUES (99, 'duplicate@example.test', 'owner');")
        start = self.coordinate()
        self.write_checkpoint(start)
        self.admin_sql(
            self.source,
            "START TRANSACTION; "
            "INSERT INTO accounts VALUES (1, 'first@example.test', 'first'); "
            "INSERT INTO accounts VALUES (2, 'duplicate@example.test', 'second'); "
            "COMMIT;",
        )
        stop = self.coordinate()
        for attempt in (1, 2):
            result = self.run_stream(start, stop, insert_conflict_policy="ignore-duplicate")
            output = f"{result.stdout}\n{result.stderr}".lower()
            if result.returncode == 0 or "row conflict persisted for repair" not in output:
                raise HarnessError(f"duplicate rollback attempt {attempt} did not fail durably: {output}")
            rows = self.admin_query(self.target, "SELECT id,email,payload FROM accounts ORDER BY id;").strip()
            if rows != "99\tduplicate@example.test\towner":
                raise HarnessError(f"duplicate rollback mutated sibling/owner rows: {rows!r}")
            checkpoint = self.checkpoint()
            if checkpoint.get("source_file") != start.file or int(checkpoint.get("source_position", 0)) != start.position:
                raise HarnessError(f"duplicate rollback advanced checkpoint: {checkpoint}")
            evidence = self.admin_query(
                self.target,
                "SELECT source_primary_key_json,error_code,attempt_count,status FROM cdc.row_conflicts "
                "WHERE table_name='accounts' ORDER BY conflict_identity;",
            ).strip()
            expected = f'["2"]\t1062\t{attempt}\tunresolved'
            if evidence != expected:
                raise HarnessError(f"duplicate evidence mismatch attempt={attempt}: {evidence!r}")

        self.admin_sql(
            self.target,
            "INSERT INTO accounts VALUES (98, 'different-pk@example.test', 'different-pk-owner');",
        )
        different_pk_start = stop
        self.write_checkpoint(different_pk_start)
        self.admin_sql(
            self.source,
            "INSERT INTO accounts VALUES (3, 'different-pk@example.test', 'different-pk');",
        )
        different_pk_stop = self.coordinate()
        different_pk_result = self.run_stream(
            different_pk_start,
            different_pk_stop,
            insert_conflict_policy="ignore-duplicate",
        )
        different_pk_output = f"{different_pk_result.stdout}\n{different_pk_result.stderr}".lower()
        if different_pk_result.returncode == 0 or "row conflict persisted for repair" not in different_pk_output:
            raise HarnessError(f"different-PK replay did not fail durably: {different_pk_output}")
        rows = self.admin_query(self.target, "SELECT id,email,payload FROM accounts ORDER BY id;").strip()
        expected_rows = (
            "98\tdifferent-pk@example.test\tdifferent-pk-owner\n"
            "99\tduplicate@example.test\towner"
        )
        if rows != expected_rows:
            raise HarnessError(f"different-PK replay mutated owner rows: {rows!r}")
        checkpoint = self.checkpoint()
        if checkpoint.get("source_file") != different_pk_start.file or int(checkpoint.get("source_position", 0)) != different_pk_start.position:
            raise HarnessError(f"different-PK replay advanced checkpoint: {checkpoint}")
        evidence = self.admin_query(
            self.target,
            "SELECT source_primary_key_json,attempt_count,status FROM cdc.row_conflicts "
            "WHERE table_name='accounts' ORDER BY source_primary_key_json;",
        ).strip().splitlines()
        if evidence != ['["2"]\t2\tunresolved', '["3"]\t1\tunresolved']:
            raise HarnessError(f"different-PK evidence was not isolated: {evidence!r}")

        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, "DROP TABLE IF EXISTS constraint_rows;")
        self.admin_sql(
            self.source,
            "CREATE TABLE constraint_rows (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL) ENGINE=InnoDB;",
        )
        self.admin_sql(
            self.target,
            "CREATE TABLE constraint_rows (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL, "
            "CONSTRAINT chk_constraint_rows_payload CHECK (payload <> 'blocked')) ENGINE=InnoDB;",
        )
        constraint_start = self.coordinate()
        self.write_checkpoint(constraint_start)
        self.admin_sql(
            self.source,
            "START TRANSACTION; "
            "INSERT INTO constraint_rows VALUES (10, 'first'); "
            "INSERT INTO constraint_rows VALUES (11, 'blocked'); "
            "COMMIT;",
        )
        constraint_stop = self.coordinate()
        for attempt in (1, 2):
            result = self.run_stream(
                constraint_start,
                constraint_stop,
                insert_conflict_policy="ignore-duplicate",
            )
            output = f"{result.stdout}\n{result.stderr}".lower()
            if result.returncode == 0 or "constraint failure" not in output and "conflict persisted for repair" not in output:
                raise HarnessError(f"constraint rollback attempt {attempt} did not fail durably: {output}")
            rows = self.admin_query(self.target, "SELECT COUNT(*) FROM constraint_rows;").strip()
            if rows != "0":
                raise HarnessError(f"constraint rollback retained sibling rows: {rows}")
            checkpoint = self.checkpoint()
            if checkpoint.get("source_file") != constraint_start.file or int(checkpoint.get("source_position", 0)) != constraint_start.position:
                raise HarnessError(f"constraint rollback advanced checkpoint: {checkpoint}")
            evidence = self.admin_query(
                self.target,
                "SELECT source_primary_key_json,error_code,attempt_count,status FROM cdc.row_conflicts "
                "WHERE table_name='constraint_rows' ORDER BY conflict_identity;",
            ).strip().split("\t")
            if len(evidence) != 4 or evidence[0] != '["11"]' or evidence[1] not in {"3819", "4025"} or evidence[2:] != [str(attempt), "unresolved"]:
                raise HarnessError(f"constraint evidence mismatch attempt={attempt}: {evidence!r}")
        print(
            "row-conflict-rollback_ok equal_pk_duplicate=continued "
            "equal_pk_checkpoint_advanced=true equal_pk_conflict_debt=0 "
            "duplicate_attempts=2 different_pk_attempts=1 "
            "constraint_attempts=2 checkpoint_unchanged=true"
        )

    def assert_foreign_keys_enabled(self) -> None:
        assert self.source and self.target
        for endpoint, label in ((self.source, "source"), (self.target, "target")):
            checks = self.admin_query(endpoint, "SELECT @@FOREIGN_KEY_CHECKS;").strip()
            if checks != "1":
                raise HarnessError(f"{label} foreign-key checks were not enabled: {checks}")

    def setup_repair_parent_child(self) -> None:
        assert self.source and self.target
        for endpoint in (self.source, self.target):
            self.admin_sql(
                endpoint,
                "DROP TABLE IF EXISTS repair_children; DROP TABLE IF EXISTS repair_parents; "
                "CREATE TABLE repair_parents (id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(64) NOT NULL) ENGINE=InnoDB; "
                "CREATE TABLE repair_children (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NOT NULL, "
                "label VARCHAR(64) NOT NULL, CONSTRAINT repair_children_parent_fk FOREIGN KEY (parent_id) "
                "REFERENCES repair_parents (id)) ENGINE=InnoDB;",
            )
        self.assert_foreign_keys_enabled()

    def setup_repair_accounts(self, table: str = "repair_accounts") -> None:
        assert self.source and self.target
        schema = (
            f"DROP TABLE IF EXISTS {table}; "
            f"CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL, "
            f"payload VARCHAR(64) NOT NULL, UNIQUE KEY uq_{table}_email (email)) ENGINE=InnoDB;"
        )
        self.admin_sql(self.source, schema)
        self.admin_sql(self.target, schema)
        self.assert_foreign_keys_enabled()

    def run_repair_scenario(self, scenario: str) -> None:
        assert self.source and self.target
        if scenario in {"fk-child-first-delete", "fk-parent-first-insert"}:
            self.setup_repair_parent_child()
            if scenario == "fk-child-first-delete":
                self.admin_sql(self.source, "INSERT INTO repair_parents VALUES (1, 'keep');")
                self.admin_sql(self.target, "INSERT INTO repair_parents VALUES (1, 'keep'), (2, 'extra');")
                self.admin_sql(self.target, "INSERT INTO repair_children VALUES (2, 2, 'extra');")
                result = self.run_repair(
                    tables=["repair_parents", "repair_children"], max_deletes=2, run_id="fk-delete-run"
                )
                require_success(result, scenario)
                parents = self.query(self.target, "SELECT id FROM repair_parents ORDER BY id;", user=TARGET_USER, password=TARGET_PASSWORD).strip()
                children = self.query(self.target, "SELECT id FROM repair_children ORDER BY id;", user=TARGET_USER, password=TARGET_PASSWORD).strip()
                if parents != "1" or children:
                    raise HarnessError(f"{scenario} did not delete child before parent: parents={parents!r} children={children!r}")
                print(f"{scenario}_converged fk_checks=1 child_before_parent=true")
                return

            self.admin_sql(self.source, "INSERT INTO repair_parents VALUES (1, 'parent');")
            self.admin_sql(self.source, "INSERT INTO repair_children VALUES (1, 1, 'child');")
            result = self.run_repair(
                tables=["repair_parents", "repair_children"], max_deletes=0, run_id="fk-insert-run"
            )
            require_success(result, scenario)
            rows = self.query(self.target, "SELECT p.id,c.parent_id FROM repair_parents p JOIN repair_children c ON c.parent_id=p.id;", user=TARGET_USER, password=TARGET_PASSWORD).strip()
            if rows != "1\t1":
                raise HarnessError(f"{scenario} did not insert parent before child: {rows!r}")
            print(f"{scenario}_converged fk_checks=1 parent_before_child=true")
            return

        if scenario == "fk-cycle-block":
            for endpoint in (self.source, self.target):
                self.admin_sql(
                    endpoint,
                    "DROP TABLE IF EXISTS repair_cycle_a; DROP TABLE IF EXISTS repair_cycle_b; "
                    "CREATE TABLE repair_cycle_a (id BIGINT NOT NULL PRIMARY KEY, b_id BIGINT NOT NULL) ENGINE=InnoDB; "
                    "CREATE TABLE repair_cycle_b (id BIGINT NOT NULL PRIMARY KEY, a_id BIGINT NOT NULL) ENGINE=InnoDB; "
                    "ALTER TABLE repair_cycle_a ADD CONSTRAINT repair_cycle_a_b_fk FOREIGN KEY (b_id) REFERENCES repair_cycle_b (id); "
                    "ALTER TABLE repair_cycle_b ADD CONSTRAINT repair_cycle_b_a_fk FOREIGN KEY (a_id) REFERENCES repair_cycle_a (id);",
                )
            self.assert_foreign_keys_enabled()
            result = self.run_repair(
                tables=["repair_cycle_a", "repair_cycle_b"], max_deletes=0, run_id="fk-cycle-run"
            )
            output = f"{result.stdout}\n{result.stderr}".lower()
            if result.returncode == 0 or "cycle" not in output:
                raise HarnessError(f"{scenario} did not block cyclic repair: {result}")
            source_constraints = self.admin_query(self.source, "SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema='globalcomix' AND table_name IN ('repair_cycle_a','repair_cycle_b');").strip()
            target_rows = self.query(self.target, "SELECT COUNT(*) FROM repair_cycle_a; SELECT COUNT(*) FROM repair_cycle_b;", user=TARGET_USER, password=TARGET_PASSWORD).strip()
            if source_constraints != "2" or target_rows != "0\n0":
                raise HarnessError(f"{scenario} mutated before cycle block: constraints={source_constraints} rows={target_rows!r}")

            self.admin_sql(self.target, "ALTER TABLE repair_cycle_b DROP FOREIGN KEY repair_cycle_b_a_fk;")
            mismatch = self.run_repair(
                tables=["repair_cycle_a", "repair_cycle_b"], max_deletes=0, run_id="fk-schema-mismatch-run"
            )
            mismatch_output = f"{mismatch.stdout}\n{mismatch.stderr}".lower()
            if mismatch.returncode == 0 or "foreign-key inventory differs" not in mismatch_output:
                raise HarnessError(f"{scenario} did not block schema mismatch: {mismatch}")
            target_rows = self.query(self.target, "SELECT COUNT(*) FROM repair_cycle_a; SELECT COUNT(*) FROM repair_cycle_b;", user=TARGET_USER, password=TARGET_PASSWORD).strip()
            if target_rows != "0\n0":
                raise HarnessError(f"{scenario} schema mismatch mutated target: {target_rows!r}")
            print(f"{scenario}_blocked cycle=true schema_mismatch=true no_mutation=true")
            return

        if scenario == "repair-resume":
            self.setup_repair_accounts("repair_resume")
            values = ",".join(f"({index}, 'resume-{index}', 'source-{index}')" for index in range(1, 4001))
            self.admin_sql(self.source, f"INSERT INTO repair_resume VALUES {values};")
            run_id = "repair-resume-run"
            process, log_path = self.start_repair(
                tables=["repair_resume"], max_deletes=0, run_id=run_id, chunk_size=10
            )
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise HarnessError(f"{scenario} exited before interruption: {log_path.read_text()}")
                try:
                    progress = self.admin_query(
                        self.target,
                        "SELECT run_id,status,chunks FROM globalcomix.table_sync_runs "
                        "WHERE run_id LIKE 'repair-resume-run-%' ORDER BY run_id;",
                    )
                except HarnessError:
                    progress = ""
                if any(
                    len(parts) == 3 and parts[1] == "running" and int(parts[2]) >= 10
                    for parts in (line.split("\t") for line in progress.splitlines() if line.strip())
                ):
                    break
                time.sleep(0.1)
            else:
                raise HarnessError(f"{scenario} did not persist an interrupted phase: {progress}")
            process.kill()
            process.wait(timeout=30)
            log = getattr(process, "_cdc_log", None)
            if log is not None:
                log.close()

            changed = self.run_repair(
                tables=["repair_resume"], max_deletes=1, run_id=run_id, chunk_size=10
            )
            changed_output = f"{changed.stdout}\n{changed.stderr}".lower()
            if changed.returncode == 0 or "immutable specification" not in changed_output:
                raise HarnessError(f"{scenario} accepted a changed plan hash: {changed}")
            resumed = self.run_repair(
                tables=["repair_resume"], max_deletes=0, run_id=run_id, chunk_size=10, timeout=240
            )
            require_success(resumed, f"{scenario} resume")
            count = self.query(self.target, "SELECT COUNT(*) FROM repair_resume;", user=TARGET_USER, password=TARGET_PASSWORD).strip()
            if count != "4000":
                raise HarnessError(f"{scenario} did not converge after resume: {count}")
            print(f"{scenario}_converged same_run_id=true changed_plan_rejected=true rows={count}")
            return

        if scenario == "bounded-delete":
            self.setup_repair_accounts("repair_bounded")
            self.admin_sql(self.source, "INSERT INTO repair_bounded VALUES (1, 'one', 'source-one'), (2, 'two', 'source-two'), (3, 'three', 'source-three'), (4, 'four', 'source-four');")
            self.admin_sql(self.target, "INSERT INTO repair_bounded VALUES (1, 'one', 'target-one'), (2, 'two', 'target-two'), (3, 'three', 'target-three'), (4, 'four', 'outside-four'), (5, 'five', 'outside-extra');")
            coordinate = self.coordinate()
            identity = self.conflict_identity(coordinate.file, coordinate.position, "repair_bounded", ["4"])
            self.admin_sql(
                self.target,
                "INSERT INTO cdc.row_conflicts "
                "(conflict_identity,source_identity,source_server_id,source_file,source_start_position,source_end_position,"
                "schema_name,table_name,operation,source_primary_key_json,duplicate_index,duplicate_owner_primary_key_json,"
                "error_code,error_text,first_observed_at_ms,last_observed_at_ms,attempt_count,status) VALUES ("
                f"{sql_literal(identity)},{sql_literal(SOURCE_IDENTITY)},101,{sql_literal(coordinate.file)},{coordinate.position},{coordinate.position + 1},"
                "'globalcomix','repair_bounded','update','[\\\"4\\\"]',NULL,NULL,1062,'outside selected window',1,1,1,'unresolved');",
            )
            result = self.run_repair(
                tables=["repair_bounded"], max_deletes=0, run_id="bounded-repair-run", start_after=["1"], end_at=["3"]
            )
            require_success(result, scenario)
            rows = self.query(self.target, "SELECT id,payload FROM repair_bounded ORDER BY id;", user=TARGET_USER, password=TARGET_PASSWORD).strip()
            expected = "1\ttarget-one\n2\tsource-two\n3\tsource-three\n4\toutside-four\n5\toutside-extra"
            if rows != expected:
                raise HarnessError(f"{scenario} mutated outside selected PK window or deleted unbounded rows: {rows!r}")
            debt = self.query(
                self.target,
                "SELECT status FROM cdc.row_conflicts "
                f"WHERE source_identity={sql_literal(SOURCE_IDENTITY)} AND table_name='repair_bounded' "
                "AND source_primary_key_json='[\\\"4\\\"]';",
                user=TARGET_USER,
                password=TARGET_PASSWORD,
            ).strip()
            if debt != "unresolved":
                raise HarnessError(f"{scenario} resolved conflict outside selected PK window: {debt!r}")
            print(f"{scenario}_converged window=(1,3] outside_rows_untouched=true conflict_outside_scope_unresolved=true max_deletes=0")
            return

        if scenario == "conflict-resolution-zero-debt":
            table = "repair_conflicts"
            self.setup_repair_accounts(table)
            self.admin_sql(self.source, f"INSERT INTO {table} VALUES (1, 'duplicate@example.test', 'source-one'), (2, 'other@example.test', 'source-two');")
            self.admin_sql(self.target, f"INSERT INTO {table} VALUES (1, 'old@example.test', 'target-one'), (2, 'duplicate@example.test', 'target-owner');")
            first = self.run_repair(tables=[table], max_deletes=0, run_id="conflict-secondary-run")
            first_output = f"{first.stdout}\n{first.stderr}".lower()
            if first.returncode == 0 or not any(
                marker in first_output for marker in ("duplicate", "verification found", "mismatched rows")
            ):
                raise HarnessError(f"secondary-unique conflict did not fail closed: {first}")
            owner = self.query(self.target, f"SELECT id,email,payload FROM {table} WHERE id=2;", user=TARGET_USER, password=TARGET_PASSWORD).strip()
            if owner != "2\tduplicate@example.test\ttarget-owner":
                raise HarnessError(f"secondary-unique conflict mutated a different primary key: {owner!r}")

            coordinate = self.coordinate()
            primary_key = ["1"]
            identity = self.conflict_identity(coordinate.file, coordinate.position, table, primary_key)
            self.admin_sql(
                self.target,
                "INSERT INTO cdc.row_conflicts "
                "(conflict_identity,source_identity,source_server_id,source_file,source_start_position,source_end_position,"
                "schema_name,table_name,operation,source_primary_key_json,duplicate_index,duplicate_owner_primary_key_json,"
                "error_code,error_text,first_observed_at_ms,last_observed_at_ms,attempt_count,status) VALUES ("
                f"{sql_literal(identity)},{sql_literal(SOURCE_IDENTITY)},101,{sql_literal(coordinate.file)},{coordinate.position},{coordinate.position + 1},"
                f"'globalcomix',{sql_literal(table)},'update','[\\\"1\\\"]','uq_{table}_email','[\\\"2\\\"]',1062,"
                f"'Duplicate entry duplicate@example.test for key uq_{table}_email',1,1,1,'unresolved');",
            )
            self.admin_sql(self.target, f"UPDATE {table} SET email='other@example.test',payload='source-two' WHERE id=2;")
            second = self.run_repair(tables=[table], max_deletes=0, run_id="conflict-resolution-run")
            require_success(second, "conflict resolution")
            debt = self.query(
                self.target,
                f"SELECT status,repair_run_id,resolution_evidence FROM cdc.row_conflicts WHERE source_identity={sql_literal(SOURCE_IDENTITY)} AND table_name={sql_literal(table)};",
                user=TARGET_USER,
                password=TARGET_PASSWORD,
            ).strip()
            expected_debt = (
                "resolved\tconflict-resolution-run\t"
                "verified source/target equality for table `repair_conflicts` across full-table scope"
            )
            if debt != expected_debt:
                raise HarnessError(f"conflict debt did not resolve with evidence: {debt!r}")
            unresolved = self.query(self.target, f"SELECT COUNT(*) FROM cdc.row_conflicts WHERE source_identity={sql_literal(SOURCE_IDENTITY)} AND table_name={sql_literal(table)} AND status='unresolved';", user=TARGET_USER, password=TARGET_PASSWORD).strip()
            if unresolved != "0":
                raise HarnessError(f"{scenario} left unresolved conflict debt: {unresolved}")
            print(f"{scenario}_converged secondary_owner_preserved=true verified_equality=true unresolved=0")
            return

        raise HarnessError(f"unknown repair scenario: {scenario}")

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
        elif scenario == "missing-conflict-trigger":
            self.admin_sql(self.target, "DROP TRIGGER cdc.row_conflicts_update_guard;")
            expected = "trigger"
        elif scenario == "missing-grant":
            self.admin_sql(self.target, "REVOKE UPDATE ON cdc.ddl_replay_journal FROM 'cdc_stream'@'%';")
            expected = "grant"
        elif scenario == "missing-conflict-table":
            self.admin_sql(self.target, "DROP TABLE cdc.row_conflicts;")
            expected = "conflict store"
        elif scenario == "wrong-conflict-schema":
            self.admin_sql(self.target, "ALTER TABLE cdc.row_conflicts MODIFY status VARCHAR(32) NOT NULL;")
            expected = "column schema"
        elif scenario == "missing-conflict-grant":
            self.admin_sql(self.target, "REVOKE UPDATE ON cdc.row_conflicts FROM 'cdc_stream'@'%';")
            expected = "grant"
        elif scenario == "broad-conflict-grant":
            self.admin_sql(self.target, "GRANT DELETE ON cdc.row_conflicts TO 'cdc_stream'@'%';")
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
        elif scenario == "production-alter-table":
            self.run_production_alter_table()
        elif scenario == "create-table-crash-restart":
            self.run_create_table_crash_restart()
        elif scenario == "bootstrap-contract":
            self.run_bootstrap_contract()
        elif scenario == "catchup-snapshot-tls":
            self.run_catchup_snapshot_tls()
        elif scenario in {
            "missing-checkpoint",
            "missing-trigger",
            "missing-conflict-trigger",
            "missing-grant",
            "missing-conflict-table",
            "wrong-conflict-schema",
            "missing-conflict-grant",
            "broad-conflict-grant",
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
        elif scenario == "replace-divergent-pk":
            self.run_replace_divergent_pk()
        elif scenario == "row-conflict-rollback":
            self.run_row_conflict_rollback()
        elif scenario == "pre-state-drift":
            self.run_journal_mismatch_scenario("pre-state-drift")
        elif scenario == "coordinate-reuse":
            self.run_journal_mismatch_scenario("coordinate-reuse")
        elif scenario == "raw-sql-reuse":
            self.run_journal_mismatch_scenario("raw-sql-reuse")
        elif scenario == "end-position-reuse":
            self.run_journal_mismatch_scenario("end-position-reuse")
        elif scenario == "checkpoint-mismatch":
            self.run_journal_mismatch_scenario("checkpoint-mismatch")
        elif scenario == "fk-child-first-delete":
            self.run_repair_scenario("fk-child-first-delete")
        elif scenario == "fk-parent-first-insert":
            self.run_repair_scenario("fk-parent-first-insert")
        elif scenario == "fk-cycle-block":
            self.run_repair_scenario("fk-cycle-block")
        elif scenario == "repair-resume":
            self.run_repair_scenario("repair-resume")
        elif scenario == "bounded-delete":
            self.run_repair_scenario("bounded-delete")
        elif scenario == "conflict-resolution-zero-debt":
            self.run_repair_scenario("conflict-resolution-zero-debt")
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
