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
import signal
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
    ScenarioSpec("sync-enum-append", True),
    ScenarioSpec("sync-enum-incompatible", True),
    ScenarioSpec("sync-constraints-preserved", True),
    ScenarioSpec("sync-parent-only-constraints-preserved", True),
    ScenarioSpec("sync-fk-parent-insert", True),
    ScenarioSpec("sync-fk-parent-update", True),
    ScenarioSpec("sync-fk-restrict-key-transition", True),
    ScenarioSpec("sync-fk-restrict-key-transition-cursor-resume", True),
    ScenarioSpec("sync-fk-restrict-key-transition-reparent", True),
    ScenarioSpec("sync-fk-restrict-key-transition-rollback-resume", True),
    ScenarioSpec("sync-fk-restrict-key-transition-new-child", True),
    ScenarioSpec("sync-fk-parent-stale-unique-owner", True),
    ScenarioSpec("sync-fk-source-absent-unique-owner", True),
    ScenarioSpec("sync-update-stale-unique-owner-rollback-resume", True),
    ScenarioSpec("sync-unique-owner-rollback-resume", True),
    ScenarioSpec("sync-wide-update", True),
    ScenarioSpec("sync-bit-values", True),
    ScenarioSpec("sync-resume", True),
    ScenarioSpec("sync-legacy-complete-resume", True),
    ScenarioSpec("sync-schema-parallel-resume", True),
    ScenarioSpec("repair-fk-orphans-parents", True),
    ScenarioSpec("repair-guest-range", True),
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
        publish_args = ["-p", f"127.0.0.1:{port}:3306"]
        if role == "target":
            publish_args.extend(["-p", f"[::1]:{port}:3306"])
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
            *publish_args,
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
        self.admin_sql_file(
            self.source, self.repo / "fixtures/cdc-harness-source-bootstrap.sql"
        )
        self.admin_sql_file(
            self.target, self.repo / "fixtures/cdc-harness-target-bootstrap.sql"
        )
        self.admin_sql_file(
            self.target, self.repo / "docs/stream-recovery-records-bootstrap.sql"
        )

    def _assert_endpoint_tls(
        self, endpoint: Endpoint, user: str, password: str, label: str
    ) -> None:
        values = self.query(
            endpoint, "SHOW STATUS LIKE 'Ssl_cipher';", user=user, password=password
        )
        rows = [line.split("\t", 1) for line in values.splitlines() if "\t" in line]
        cipher = next((value for name, value in rows if name == "Ssl_cipher"), "")
        if not cipher:
            raise HarnessError(
                f"{label} TLS identity/cipher validation was not observable: {values!r}"
            )
        print(
            f"endpoint_tls_diagnostics label={label} port={endpoint.port} cipher={cipher}"
        )

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
            (frozenset({"SELECT", "INSERT", "UPDATE"}), "cdc.stream_recovery_records"),
            (
                frozenset({"EXECUTE"}),
                "PROCEDURE cdc.ddl_replay_journal_trigger_inventory",
            ),
            (
                frozenset({"EXECUTE"}),
                "PROCEDURE cdc.stream_recovery_records_trigger_inventory",
            ),
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
            (frozenset({"SELECT", "INSERT", "UPDATE"}), "cdc.sync_runs_phases"),
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
        target_host: str = "127.0.0.1",
        target_ca_file: Path | None = None,
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
            target_host,
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
        target_host: str = "127.0.0.1",
        timeout: float = 180,
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
                target_host=target_host,
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
        target_host: str = "127.0.0.1",
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
                target_host=target_host,
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

        releases_schema = """
            CREATE TABLE releases (
                id BIGINT NOT NULL PRIMARY KEY,
                is_deleted TINYINT NOT NULL,
                is_published TINYINT NOT NULL,
                is_visible TINYINT NOT NULL,
                comic_is_visible TINYINT NOT NULL,
                lang_id BIGINT NOT NULL,
                published_time DATETIME NOT NULL,
                comic_id BIGINT NOT NULL,
                title VARCHAR(64) NOT NULL,
                KEY idx_downloads_sort (
                    is_deleted,
                    is_published,
                    is_visible,
                    comic_is_visible,
                    lang_id,
                    published_time
                )
            ) ENGINE=InnoDB;
            INSERT INTO releases VALUES
                (1, 0, 1, 1, 1, 1, '2026-08-28 12:00:00', 11, 'first'),
                (2, 0, 1, 1, 1, 1, '2026-08-28 13:00:00', 10, 'second'),
                (3, 1, 0, 0, 0, 2, '2026-08-28 14:00:00', 12, 'third');
        """
        self.admin_sql(self.source, releases_schema)
        self.admin_sql(self.target, releases_schema)
        releases_start = self.coordinate()
        self.write_checkpoint(releases_start)
        self.admin_sql(
            self.source,
            """
            ALTER TABLE releases
              DROP INDEX idx_downloads_sort,
              ADD INDEX idx_downloads_sort (
                is_deleted,
                is_published,
                is_visible,
                comic_is_visible,
                lang_id,
                published_time DESC,
                comic_id ASC,
                id ASC
              ),
              ALGORITHM=INPLACE,
              LOCK=NONE;
            """,
        )
        releases_stop = self.coordinate()
        releases_process, releases_log = self.start_stream(releases_start, releases_stop)
        deadline = time.monotonic() + 30
        while releases_process.poll() is None and time.monotonic() < deadline:
            pending = self.query(
                self.target,
                "SELECT status FROM cdc.ddl_replay_journal "
                "WHERE raw_sql LIKE 'ALTER TABLE releases%idx_downloads_sort%' "
                "ORDER BY event_start_position DESC LIMIT 1;",
                user=TARGET_USER,
                password=TARGET_PASSWORD,
            ).strip()
            if pending == "translation_pending":
                self.stop_sync_process(releases_process)
                raise HarnessError(
                    "production releases index rebuild persisted translation_pending; "
                    f"translator upgrade required: {releases_log.read_text()}"
                )
            time.sleep(0.05)
        if releases_process.poll() is None:
            self.stop_sync_process(releases_process)
            raise HarnessError(
                "production releases index rebuild did not finish or persist a translation-pending barrier: "
                f"{releases_log.read_text()}"
            )
        releases_result = self.finish_stream(releases_process)
        require_success(releases_result, "production releases index rebuild replay")
        releases_rows = self.query(
            self.target,
            "SELECT id,is_deleted,is_published,is_visible,comic_is_visible,lang_id,"
            "DATE_FORMAT(published_time, '%Y-%m-%d %H:%i:%s'),comic_id,title "
            "FROM releases ORDER BY id;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).splitlines()
        expected_releases_rows = [
            "1\t0\t1\t1\t1\t1\t2026-08-28 12:00:00\t11\tfirst",
            "2\t0\t1\t1\t1\t1\t2026-08-28 13:00:00\t10\tsecond",
            "3\t1\t0\t0\t0\t2\t2026-08-28 14:00:00\t12\tthird",
        ]
        if releases_rows != expected_releases_rows:
            raise HarnessError(f"production releases index rebuild changed rows: {releases_rows!r}")
        releases_index = self.query(
            self.target,
            "SELECT seq_in_index,column_name,collation FROM information_schema.statistics "
            "WHERE table_schema='globalcomix' AND table_name='releases' "
            "AND index_name='idx_downloads_sort' ORDER BY seq_in_index;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).splitlines()
        expected_releases_index = [
            "1\tis_deleted\tA",
            "2\tis_published\tA",
            "3\tis_visible\tA",
            "4\tcomic_is_visible\tA",
            "5\tlang_id\tA",
            "6\tpublished_time\tD",
            "7\tcomic_id\tA",
            "8\tid\tA",
        ]
        if releases_index != expected_releases_index:
            raise HarnessError(
                f"production releases index rebuild metadata mismatch: {releases_index!r}"
            )
        releases_journal = self.query(
            self.target,
            "SELECT status FROM cdc.ddl_replay_journal "
            "WHERE raw_sql LIKE 'ALTER TABLE releases%idx_downloads_sort%' "
            "ORDER BY event_start_position DESC LIMIT 1;",
            user=TARGET_USER,
            password=TARGET_PASSWORD,
        ).strip()
        if releases_journal != "checkpointed":
            raise HarnessError(
                f"production releases index rebuild journal mismatch: {releases_journal!r}"
            )
        releases_checkpoint = self.checkpoint()
        if releases_checkpoint.get("source_file") != releases_stop.file or int(
            releases_checkpoint.get("source_position", 0)
        ) != releases_stop.position:
            raise HarnessError(
                f"production releases index rebuild checkpoint mismatch: {releases_checkpoint}"
            )

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
        pending_process, pending_log = self.start_stream(no_op_stop, pending_stop)
        try:
            self.wait_for_pending_ddl(pending_process, pending_log, "uq_accounts_email_prefix")
        finally:
            self.stop_sync_process(pending_process)
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
            "checkpointed_ddl_rows=6 unique_parity=true drop_column=true drop_noop=true "
            "releases_index_rebuild=true pending_unique_option=true"
        )

    def wait_for_pending_ddl(
        self, process: subprocess.Popen[str], log: Path, statement_marker: str
    ) -> None:
        deadline = time.monotonic() + 30
        query = (
            "SELECT status FROM cdc.ddl_replay_journal WHERE raw_sql LIKE "
            f"{sql_literal('%' + statement_marker + '%')} "
            "ORDER BY event_start_position DESC LIMIT 1;"
        )
        while process.poll() is None and time.monotonic() < deadline:
            status = self.admin_query(self.target, query).strip()
            if status == "translation_pending":
                if process.poll() is not None:
                    raise HarnessError(f"pending DDL process exited: {log.read_text()}")
                return
            time.sleep(0.05)
        raise HarnessError(
            f"process did not stay live with pending DDL: {log.read_text()}"
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

    def seed_sync_enum_evolution(self, table: str, source_labels: list[str]) -> None:
        assert self.source and self.target
        original = ["success_with_facts", "success_empty", "excluded", "failed"]
        for endpoint, labels, collation in (
            (self.source, source_labels, "utf8mb4_uca1400_ai_ci"),
            (self.target, original, "utf8mb4_0900_ai_ci"),
        ):
            declaration = ",".join(sql_literal(label) for label in labels)
            self.admin_sql(
                endpoint,
                f"CREATE TABLE `{table}` (id INT PRIMARY KEY, "
                f"status ENUM({declaration}) NOT NULL) "
                f"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE={collation};",
            )
            values = ",".join(
                f"({index},{sql_literal(label)})"
                for index, label in enumerate(labels[:4], 1)
            )
            self.admin_sql(endpoint, f"INSERT INTO `{table}` VALUES {values};")

    def run_sync_enum_append(self) -> None:
        assert self.source and self.target
        table = "sync_enum_append_rows"
        run_id = "sync-enum-append"
        labels = ["success_with_facts", "success_empty", "excluded", "failed", "stale"]
        self.seed_sync_enum_evolution(table, labels)
        result = self.run_sync(tables=[table], run_id=run_id, chunk_size=2)
        require_success(result, "append ENUM label through staged sync")
        snapshot_sql = f"SELECT id,status,status+0 FROM `{table}` ORDER BY id;"
        expected = "\n".join(
            f"{i}\t{label}\t{i}" for i, label in enumerate(labels[:4], 1)
        )
        if self.admin_query(self.target, snapshot_sql).strip() != expected:
            raise HarnessError("ENUM append changed original labels or ordinals")
        progress_sql = (
            "SELECT stage,status,last_primary_key_json,chunks,rows_scanned,updated_at "
            f"FROM cdc.sync_runs WHERE run_id={sql_literal(run_id)} ORDER BY stage;"
        )
        before = self.admin_query(self.target, progress_sql)
        stages = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} AND status='complete';",
        ).strip()
        if stages != "3":
            raise HarnessError(f"ENUM append did not complete all stages: {stages}")
        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, f"INSERT INTO `{table}` VALUES (5,'stale');")
            if (
                self.admin_query(
                    endpoint, f"SELECT status,status+0 FROM `{table}` WHERE id=5;"
                ).strip()
                != "stale\t5"
            ):
                raise HarnessError(
                    "appended ENUM label is not writable with ordinal five"
                )
        resumed = self.run_sync(tables=[table], run_id=run_id, chunk_size=2)
        require_success(resumed, "same-run ENUM append resume")
        if self.admin_query(self.target, progress_sql) != before:
            raise HarnessError("same-run ENUM append resume rewrote completed progress")
        expected += "\n5\tstale\t5"
        if self.admin_query(self.target, snapshot_sql).strip() != expected:
            raise HarnessError("same-run ENUM append resume changed stored labels")
        print(
            "sync_enum_append_ok original_ordinals=4 appended_ordinal=5 stages=3 progress_preserved=true"
        )

    def run_sync_enum_incompatible(self) -> None:
        assert self.target
        original = ["success_with_facts", "success_empty", "excluded", "failed"]
        for suffix, labels in (
            ("reorder", [original[1], original[0], *original[2:]]),
            ("remove", original[:3]),
        ):
            table = f"sync_enum_{suffix}_rows"
            self.seed_sync_enum_evolution(table, labels)
            if suffix == "remove":
                self.admin_sql(self.target, f"DELETE FROM `{table}` WHERE id=4;")
            snapshot_sql = f"SELECT id,status,status+0 FROM `{table}` ORDER BY id;"
            before = self.admin_query(self.target, snapshot_sql)
            result = self.run_sync(
                tables=[table], run_id=f"sync-enum-{suffix}", chunk_size=2
            )
            if result.returncode == 0:
                raise HarnessError(f"incompatible ENUM {suffix} unexpectedly succeeded")
            if self.admin_query(self.target, snapshot_sql) != before:
                raise HarnessError(f"incompatible ENUM {suffix} mutated target rows")
            self.admin_sql(self.target, f"INSERT INTO `{table}` VALUES (10,'failed');")
            if (
                self.admin_query(
                    self.target, f"SELECT status,status+0 FROM `{table}` WHERE id=10;"
                ).strip()
                != "failed\t4"
            ):
                raise HarnessError(
                    f"incompatible ENUM {suffix} changed target declaration"
                )
        print(
            "sync_enum_incompatible_ok reorder=refused remove=refused target_unchanged=true"
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
        if progress != 'complete	["13553","loved"]	3	3	1	1	0':
            raise HarnessError(f"composite ENUM progress is wrong: {progress!r}")
        print(
            "sync_composite_enum_primary_key_ok "
            "rows_scanned=3 inserts=1 updates=1 deletes=0"
        )

    def run_sync_constraints_preserved(self, parent_only: bool = False) -> None:
        assert self.source and self.target
        for endpoint in (self.source, self.target):
            self.admin_sql(
                endpoint,
                "CREATE TABLE preserve_parent (id INT PRIMARY KEY) ENGINE=InnoDB; "
                "CREATE TABLE preserve_child (id INT PRIMARY KEY, parent_id INT, "
                "CONSTRAINT preserve_child_parent FOREIGN KEY (parent_id) "
                "REFERENCES preserve_parent(id), "
                "CONSTRAINT preserve_child_positive CHECK (id > 0)) ENGINE=InnoDB; "
                "INSERT INTO preserve_parent VALUES (1); "
                "INSERT INTO preserve_child VALUES (1,1);",
            )
        self.admin_sql(
            self.target,
            f"REVOKE ALTER ON `{APP_SCHEMA}`.* FROM '{SYNC_TARGET_USER}'@'%';",
        )
        result = self.run_sync(
            tables=["preserve_parent"]
            if parent_only
            else ["preserve_parent", "preserve_child"],
            run_id="sync-constraints-preserved",
            parallelism=2,
        )
        require_success(
            result, "sync must preserve valid constraints without ALTER privilege"
        )
        rows = self.admin_query(
            self.target, "SELECT id,parent_id FROM preserve_child;"
        ).strip()
        if rows != "1\t1":
            raise HarnessError(f"preserved child rows differ: {rows!r}")
        print("sync_constraints_preserved=true alter_privilege=false")

    def assert_key_transition_rows(
        self, endpoint: Endpoint, expected: dict[str, str]
    ) -> None:
        for table, rows in expected.items():
            actual = self.admin_query(
                endpoint, f"SELECT * FROM `{table}` ORDER BY id;"
            ).strip()
            if actual != rows:
                raise HarnessError(
                    f"key transition {endpoint.container} {table}: {actual!r} != {rows!r}"
                )

    def run_sync_fk_restrict_key_transition(self, variant: str = "") -> None:
        assert self.source and self.target
        reparent = variant == "reparent"
        run_id = "sync-fk-restrict-key-transition" + (f"-{variant}" if variant else "")
        schema = (
            "DROP TABLE IF EXISTS favorite_notes; DROP TABLE IF EXISTS favorites; "
            "DROP TABLE IF EXISTS users; "
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, "
            "UNIQUE KEY uq_users_id_name (id,name)) ENGINE=InnoDB; "
            "CREATE TABLE favorites (id INT PRIMARY KEY, user_id INT NOT NULL, "
            "user_name VARCHAR(64) NOT NULL, "
            "CONSTRAINT fk_favorites_user FOREIGN KEY (user_id,user_name) "
            "REFERENCES users(id,name) ON UPDATE RESTRICT ON DELETE RESTRICT) "
            "ENGINE=InnoDB; "
            "CREATE TABLE favorite_notes (id INT PRIMARY KEY, favorite_id INT NOT NULL, "
            "CONSTRAINT fk_notes_favorite FOREIGN KEY (favorite_id) "
            "REFERENCES favorites(id) ON UPDATE RESTRICT ON DELETE RESTRICT) "
            "ENGINE=InnoDB; "
        )
        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, schema)
        desired_child = "10,2,'other'" if reparent else "10,1,'new'"
        # Construct valid source state directly: this fixture represents target drift.
        self.admin_sql(
            self.source,
            "INSERT INTO users VALUES (1,'new'),(2,'other'); "
            f"INSERT INTO favorites VALUES ({desired_child})"
            + (",(11,1,'new')" if variant == "new-child" else "")
            + "; "
            "INSERT INTO favorite_notes VALUES (100,10);",
        )
        self.admin_sql(
            self.target,
            "INSERT INTO users VALUES (1,'old'),(2,'other'); "
            "INSERT INTO favorites VALUES (10,1,'old'); "
            "INSERT INTO favorite_notes VALUES (100,10);",
        )
        probes = [
            "UPDATE users SET name='blocked' WHERE id=1;",
            "DELETE FROM users WHERE id=1;",
            "UPDATE favorites SET id=11 WHERE id=10;",
            "DELETE FROM favorites WHERE id=10;",
        ]
        for sql in probes:
            self.assert_admin_sql_rejected(self.target, sql, "1451")
        self.admin_sql(
            self.target,
            f"REVOKE ALTER ON `{APP_SCHEMA}`.* FROM '{SYNC_TARGET_USER}'@'%';",
        )
        if variant == "cursor-resume":
            self.admin_sql(
                self.target,
                "CREATE TABLE transition_audit (id INT AUTO_INCREMENT PRIMARY KEY, child_id INT); "
                "CREATE TRIGGER favorites_restore_audit AFTER INSERT ON favorites "
                "FOR EACH ROW INSERT INTO transition_audit(child_id) VALUES (NEW.id);\n"
                "DELIMITER //\n"
                "CREATE TRIGGER cdc.phase_cursor_failure BEFORE INSERT ON cdc.sync_runs_phases "
                "FOR EACH ROW BEGIN IF NEW.phase='update_divergent' AND NEW.table_name='users' THEN "
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected phase cursor failure'; "
                "END IF; END//\nDELIMITER ;\n",
            )
            failed = self.run_sync(
                tables=["users", "favorites", "favorite_notes"],
                run_id=run_id,
                chunk_size=1,
                parallelism=2,
                timeout=60,
            )
            if (
                failed.returncode == 0
                or "injected phase cursor failure" not in failed.stderr
            ):
                raise HarnessError(f"phase cursor failure not reached: {failed.stderr}")
            self.assert_key_transition_rows(
                self.target,
                {
                    "users": "1\tnew\n2\tother",
                    "favorites": "10\t1\tnew",
                    "favorite_notes": "100\t10",
                },
            )
            self.admin_sql(self.target, "DROP TRIGGER cdc.phase_cursor_failure;")
        if variant == "rollback-resume":
            failure_message = "injected key transition restoration failure"
            self.admin_sql(
                self.target,
                "DELIMITER //\n"
                "CREATE TRIGGER favorites_restore_failure BEFORE INSERT ON favorites "
                "FOR EACH ROW BEGIN IF NEW.id=10 AND NEW.user_name='new' THEN "
                f"SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='{failure_message}'; "
                "END IF; END//\nDELIMITER ;\n",
            )
            failed = self.run_sync(
                tables=["users", "favorites", "favorite_notes"],
                run_id=run_id,
                chunk_size=1,
                parallelism=2,
                timeout=60,
            )
            output = f"{failed.stdout}\n{failed.stderr}"
            if failed.returncode == 0 or failure_message not in output:
                raise HarnessError(
                    "restoration failure not observed: "
                    f"exit={failed.returncode} output={output!r}"
                )
            self.assert_key_transition_rows(
                self.target,
                {
                    "users": "1\told\n2\tother",
                    "favorites": "10\t1\told",
                    "favorite_notes": "100\t10",
                },
            )
            progress = self.admin_query(
                self.target,
                "SELECT status FROM cdc.sync_runs "
                f"WHERE run_id={sql_literal(run_id)} "
                "AND stage='rows' AND table_name='users';",
            ).strip()
            if progress == "complete":
                raise HarnessError(
                    f"root Rows progress must be incomplete: {progress!r}"
                )
            print(f"{run_id}_rollback_ok original_rows=true root_rows={progress}")
            self.admin_sql(self.target, "DROP TRIGGER favorites_restore_failure;")
        result = self.run_sync(
            tables=["users", "favorites", "favorite_notes"],
            run_id=run_id,
            chunk_size=1,
            parallelism=2,
            timeout=60,
        )
        print(f"{run_id} exit={result.returncode}\n{result.stdout}\n{result.stderr}")
        require_success(result, f"{run_id} with RESTRICT and no ALTER privilege")
        expected = {
            "users": "1\tnew\n2\tother",
            "favorites": "10\t2\tother" if reparent else "10\t1\tnew",
            "favorite_notes": "100\t10",
        }
        if variant == "new-child":
            expected["favorites"] += "\n11\t1\tnew"
        for endpoint in (self.source, self.target):
            self.assert_key_transition_rows(endpoint, expected)
        if variant == "cursor-resume":
            audit = self.admin_query(
                self.target, "SELECT COUNT(*) FROM transition_audit;"
            ).strip()
            if audit != "1":
                raise HarnessError(
                    f"resume repeated committed restoration: audit rows={audit}"
                )
        parent_id = 2 if reparent else 1
        for sql in (
            f"UPDATE users SET name='blocked' WHERE id={parent_id};",
            f"DELETE FROM users WHERE id={parent_id};",
            "UPDATE favorites SET id=12 WHERE id=10;",
            "DELETE FROM favorites WHERE id=10;",
        ):
            self.assert_admin_sql_rejected(self.target, sql, "1451")
        for sql in (
            "INSERT INTO favorites VALUES (20,99,'missing');",
            "INSERT INTO favorite_notes VALUES (200,99);",
        ):
            self.assert_admin_sql_rejected(self.target, sql, "1452")
        print(f"{run_id}_ok exact_rows=true fk_enforced=true alter_privilege=false")

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

    def run_sync_fk_source_absent_unique_owner(self) -> None:
        assert self.source and self.target
        run_id = "sync-fk-source-absent-unique-owner"
        schema = (
            "DROP TABLE IF EXISTS favorite_notes; DROP TABLE IF EXISTS favorites; "
            "DROP TABLE IF EXISTS users; "
            "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(64) NOT NULL, "
            "UNIQUE KEY uq_users_email (email)) ENGINE=InnoDB; "
            "CREATE TABLE favorites (id INT PRIMARY KEY, user_id INT NOT NULL, "
            "CONSTRAINT fk_favorites_user FOREIGN KEY (user_id) REFERENCES users(id) "
            "ON UPDATE RESTRICT ON DELETE RESTRICT) ENGINE=InnoDB; "
            "CREATE TABLE favorite_notes (id INT PRIMARY KEY, favorite_id INT NOT NULL, "
            "CONSTRAINT fk_notes_favorite FOREIGN KEY (favorite_id) "
            "REFERENCES favorites(id) ON UPDATE RESTRICT ON DELETE RESTRICT) "
            "ENGINE=InnoDB; "
        )
        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, schema)
        # The unique owner is absent at source, not merely holding a changed email.
        self.admin_sql(
            self.source,
            "INSERT INTO users VALUES (2,'wanted@example.test'); "
            "INSERT INTO favorites VALUES (10,2); "
            "INSERT INTO favorite_notes VALUES (100,10);",
        )
        self.admin_sql(
            self.target,
            "INSERT INTO users VALUES (1,'wanted@example.test'); "
            "INSERT INTO favorites VALUES (10,1); "
            "INSERT INTO favorite_notes VALUES (100,10);",
        )
        expected = {
            "users": "2\twanted@example.test",
            "favorites": "10\t2",
            "favorite_notes": "100\t10",
        }
        self.assert_key_transition_rows(self.source, expected)
        self.assert_key_transition_rows(
            self.target,
            {**expected, "users": "1\twanted@example.test", "favorites": "10\t1"},
        )
        self.assert_admin_sql_rejected(
            self.target, "INSERT INTO users VALUES (2,'wanted@example.test');", "1062"
        )
        for sql in (
            "DELETE FROM users WHERE id=1;",
            "DELETE FROM favorites WHERE id=10;",
        ):
            self.assert_admin_sql_rejected(self.target, sql, "1451")
        self.admin_sql(
            self.target,
            f"REVOKE ALTER ON `{APP_SCHEMA}`.* FROM '{SYNC_TARGET_USER}'@'%';",
        )
        result = self.run_sync(
            tables=["users", "favorites", "favorite_notes"],
            run_id=run_id,
            chunk_size=1,
            timeout=60,
        )
        print(f"{run_id} exit={result.returncode}\n{result.stdout}\n{result.stderr}")
        require_success(result, f"{run_id} with RESTRICT and no ALTER privilege")
        for endpoint in (self.source, self.target):
            self.assert_key_transition_rows(endpoint, expected)
        for sql in (
            "DELETE FROM users WHERE id=2;",
            "UPDATE users SET id=3 WHERE id=2;",
            "DELETE FROM favorites WHERE id=10;",
            "UPDATE favorites SET id=11 WHERE id=10;",
        ):
            self.assert_admin_sql_rejected(self.target, sql, "1451")
        for sql in (
            "INSERT INTO favorites VALUES (20,99);",
            "INSERT INTO favorite_notes VALUES (200,99);",
        ):
            self.assert_admin_sql_rejected(self.target, sql, "1452")
        self.assert_admin_sql_rejected(
            self.target, "INSERT INTO users VALUES (3,'wanted@example.test');", "1062"
        )
        print(f"{run_id}_ok exact_rows=true fk_enforced=true alter_privilege=false")

    def run_sync_update_stale_unique_owner_rollback_resume(self) -> None:
        assert self.source and self.target
        table = "users"
        run_id = "sync-update-stale-unique-owner-rollback-resume"
        schema = (
            f"DROP TABLE IF EXISTS {table}; "
            f"CREATE TABLE {table} ("
            "id INT UNSIGNED NOT NULL PRIMARY KEY, "
            "email VARCHAR(64) NOT NULL, "
            "payload VARCHAR(64) NOT NULL, "
            "UNIQUE KEY uq_users_email (email)"
            ") ENGINE=InnoDB;"
        )
        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, schema)
        self.admin_sql(
            self.source,
            "INSERT INTO users VALUES "
            "(1, 'anchor-1@example.test', 'anchor-1'), "
            "(2, 'anchor-2@example.test', 'anchor-2'), "
            "(90000, 'anchor-90000@example.test', 'anchor-90000'), "
            "(98150, 'wanted@example.test', 'source-98150'), "
            "(98151, 'self-owned@example.test', 'source-98151'), "
            "(98152, 'anchor-98152@example.test', 'anchor-98152'), "
            "(115537, 'later-owner-current@example.test', 'source-115537');",
        )
        self.admin_sql(
            self.target,
            "INSERT INTO users VALUES "
            "(1, 'anchor-1@example.test', 'anchor-1'), "
            "(2, 'anchor-2@example.test', 'anchor-2'), "
            "(90000, 'anchor-90000@example.test', 'anchor-90000'), "
            "(98150, 'old-98150@example.test', 'target-98150'), "
            "(98151, 'self-owned@example.test', 'target-98151'), "
            "(98152, 'anchor-98152@example.test', 'anchor-98152'), "
            "(115537, 'wanted@example.test', 'target-stale-owner');",
        )
        self.admin_sql(
            self.target,
            "DELIMITER //\n"
            "CREATE TRIGGER users_owner_repaired AFTER UPDATE ON users FOR EACH ROW\n"
            "BEGIN\n"
            "  IF NEW.id=115537 THEN SET @sync_owner_repaired=1; END IF;\n"
            "END//\n"
            "CREATE TRIGGER users_retry_failure BEFORE UPDATE ON users FOR EACH ROW\n"
            "BEGIN\n"
            "  IF NEW.id=98150 AND COALESCE(@sync_owner_repaired,0)=1 THEN\n"
            "    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected stale-owner update retry failure';\n"
            "  END IF;\n"
            "END//\n"
            "DELIMITER ;\n",
        )

        failed = self.run_sync(tables=[table], run_id=run_id, chunk_size=3)
        failed_output = "\n".join((failed.stdout, failed.stderr))
        if (
            failed.returncode == 0
            or "injected stale-owner update retry failure" not in failed_output
        ):
            raise HarnessError(
                "stale unique-owner UPDATE retry failure was not observed: "
                f"exit={failed.returncode} output={failed_output!r}"
            )
        retained_rows = self.admin_query(
            self.target,
            "SELECT id,email,payload FROM users ORDER BY id;",
        ).strip()
        expected_retained_rows = (
            "1\tanchor-1@example.test\tanchor-1\n"
            "2\tanchor-2@example.test\tanchor-2\n"
            "90000\tanchor-90000@example.test\tanchor-90000\n"
            "98150\told-98150@example.test\ttarget-98150\n"
            "98151\tself-owned@example.test\ttarget-98151\n"
            "98152\tanchor-98152@example.test\tanchor-98152\n"
            "115537\twanted@example.test\ttarget-stale-owner"
        )
        if retained_rows != expected_retained_rows:
            raise HarnessError(
                "failed stale unique-owner UPDATE did not roll back target rows: "
                f"{retained_rows!r}"
            )
        failed_progress = self.admin_query(
            self.target,
            "SELECT status,last_primary_key_json,chunks,rows_scanned,inserts_applied,"
            "updates_applied,deletes_applied,IF(last_error IS NULL,'<NULL>',last_error) "
            "FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} AND stage='rows' AND table_name='users';",
        ).strip()
        if failed_progress != 'running\t["90000"]\t1\t3\t0\t0\t0\t<NULL>':
            raise HarnessError(
                "failed stale unique-owner UPDATE did not retain only the prior page: "
                f"{failed_progress!r}"
            )
        if self.sync_unique_owner_audits(failed):
            raise HarnessError("rolled-back stale unique-owner UPDATE emitted an audit")

        self.admin_sql(
            self.target,
            "DROP TRIGGER users_retry_failure; DROP TRIGGER users_owner_repaired;",
        )
        resumed = self.run_sync(tables=[table], run_id=run_id, chunk_size=3)
        require_success(resumed, "resumed stale unique-owner UPDATE sync")
        source_rows = self.admin_query(
            self.source,
            "SELECT id,email,payload FROM users ORDER BY id;",
        ).strip()
        target_rows = self.admin_query(
            self.target,
            "SELECT id,email,payload FROM users ORDER BY id;",
        ).strip()
        if target_rows != source_rows:
            raise HarnessError(
                "resumed stale unique-owner UPDATE did not converge source rows: "
                f"source={source_rows!r} target={target_rows!r}"
            )
        resumed_progress = self.admin_query(
            self.target,
            "SELECT status,last_primary_key_json,chunks,rows_scanned,updates_applied "
            "FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} AND stage='rows' AND table_name='users';",
        ).strip()
        if resumed_progress != 'complete\t["115537"]\t4\t7\t2':
            raise HarnessError(
                "resumed stale unique-owner UPDATE progress mismatch: "
                f"{resumed_progress!r}"
            )
        audits = self.sync_unique_owner_audits(resumed)
        expected_audit = {
            "event": "sync_unique_owner_reconciliation",
            "table": "users",
            "index": "uq_users_email",
            "action": "update",
            "intended_primary_key": ["98150"],
            "owner_primary_key": ["115537"],
        }
        if audits != [expected_audit]:
            raise HarnessError(
                f"unexpected stale unique-owner UPDATE audits: {audits!r}"
            )
        print(
            "sync_update_stale_unique_owner_rollback_resume_ok "
            "cursor=90000 conflict=98150 owner=115537 self_owner=98151 "
            "updates=2 rollback=true resumed=true"
        )

    def reset_target_general_log(self) -> None:
        assert self.target
        self.admin_sql(
            self.target,
            "SET GLOBAL general_log=OFF; TRUNCATE TABLE mysql.general_log; "
            "SET GLOBAL log_output='TABLE'; SET GLOBAL general_log=ON;",
        )

    def sync_unique_owner_insert_sequence(self, table: str) -> list[str]:
        assert self.target
        records = self.admin_query(
            self.target,
            "SELECT command_type,argument FROM mysql.general_log "
            "WHERE user_host LIKE 'cdc_sync%' "
            f"AND LOWER(argument) LIKE '%{table}%' ORDER BY event_time;",
        ).splitlines()
        sequence = []
        for record in records:
            command_type, statement = record.split("\t", 1)
            normalized = " ".join(statement.lower().split())
            if not normalized.startswith(f"insert into `{table}`"):
                continue
            if "payload-001" in normalized and "payload-128" in normalized:
                sequence.append("first")
            elif "payload-129" in normalized and "payload-130" in normalized:
                sequence.append("second")
            elif command_type != "Prepare":
                raise HarnessError(
                    f"unclassified strict INSERT in target general log: {normalized!r}"
                )
        if not sequence:
            raise HarnessError(
                f"target general log has no value-bearing strict INSERTs: {records!r}"
            )
        return sequence

    def sync_unique_owner_transaction_sequences(self, table: str) -> list[list[str]]:
        assert self.target
        records = self.admin_query(
            self.target,
            "SELECT thread_id,command_type,argument FROM mysql.general_log "
            "WHERE user_host LIKE 'cdc_sync%' ORDER BY event_time,thread_id;",
        ).splitlines()
        transactions: list[list[str]] = []
        active_by_thread: dict[str, list[str]] = {}
        owner_primary_key_predicate = re.compile(
            r"\bwhere `id` in \('200'\) order by `id`$"
        )
        for record in records:
            thread_id, command_type, statement = record.split("\t", 2)
            if command_type not in {"Query", "Execute"}:
                continue
            normalized = " ".join(statement.lower().split()).rstrip(";")
            if normalized in {"begin", "set autocommit=0"} or normalized.startswith(
                "start transaction"
            ):
                active_by_thread[thread_id] = ["transaction_start"]
                continue
            transaction = active_by_thread.get(thread_id)
            if transaction is None:
                continue

            event = None
            if normalized == f"lock tables `globalcomix`.`{table}` write":
                event = "table_write_lock"
            elif normalized == "commit" or normalized.startswith("commit "):
                event = "commit"
            elif normalized == "rollback" or normalized.startswith("rollback "):
                event = "rollback"
            elif normalized.startswith(f"insert into `{table}`"):
                if "payload-001" in normalized and "payload-128" in normalized:
                    event = "first_batch_insert"
                elif "payload-129" in normalized and "payload-130" in normalized:
                    event = (
                        "colliding_insert"
                        if "colliding_insert" not in transaction
                        else "retry_insert"
                    )
            elif (
                normalized.startswith(f"update `{table}` set")
                and owner_primary_key_predicate.search(normalized)
                and all(
                    value in normalized
                    for value in ("token-200", "page-200", "payload-200")
                )
            ):
                event = "owner_update"
            elif normalized.startswith(
                f"delete from `{table}`"
            ) and owner_primary_key_predicate.search(normalized):
                event = "owner_delete"

            if event is None:
                continue
            transaction.append(event)
            if event in {"commit", "rollback"}:
                transactions.append(transaction)
                del active_by_thread[thread_id]
        return transactions

    def assert_sync_unique_owner_transaction(
        self,
        table: str,
        *,
        owner_action: str,
        outcome: str,
    ) -> list[str]:
        expected = [
            "transaction_start",
            "table_write_lock",
            "first_batch_insert",
            "colliding_insert",
            f"owner_{owner_action}",
            "retry_insert",
            outcome,
        ]
        transactions = self.sync_unique_owner_transaction_sequences(table)
        matches = [transaction for transaction in transactions if transaction == expected]
        if len(matches) != 1:
            raise HarnessError(
                "unique-owner transaction ordering mismatch: "
                f"expected={expected!r} transactions={transactions!r}"
            )
        return matches[0]

    def sync_unique_owner_audits(self, result: CommandResult) -> list[dict]:
        audits = []
        for line in "\n".join((result.stdout, result.stderr)).splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if value.get("event") == "sync_unique_owner_reconciliation":
                audits.append(value)
        return audits

    def run_sync_unique_owner_rollback_resume(self) -> None:
        assert self.source and self.target
        table = "sync_unique_owner_rows"
        run_id = "sync-unique-owner-rollback-resume"
        schema = (
            f"DROP TABLE IF EXISTS {table}; "
            f"CREATE TABLE {table} ("
            "id CHAR(3) NOT NULL PRIMARY KEY, "
            "token VARCHAR(64) NOT NULL, "
            "page VARCHAR(64) NOT NULL, "
            "payload VARCHAR(64) NOT NULL, "
            "UNIQUE KEY uidx_token_page (token,page)"
            ") ENGINE=InnoDB;"
        )
        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, schema)
        source_rows = [
            (
                f"{row_id:03}",
                f"token-{row_id:03}",
                f"page-{row_id:03}",
                f"payload-{row_id:03}",
            )
            for row_id in range(1, 131)
        ]
        source_rows.append(("200", "token-200", "page-200", "payload-200"))
        values = ",".join(
            f"({sql_literal(row_id)},{sql_literal(token)},{sql_literal(page)},{sql_literal(payload)})"
            for row_id, token, page, payload in source_rows
        )
        self.admin_sql(self.source, f"INSERT INTO {table} VALUES {values};")
        self.admin_sql(
            self.target,
            f"INSERT INTO {table} VALUES "
            "(200,'token-129','page-129','misfiled-owner');",
        )
        self.admin_sql(
            self.target,
            f"DELIMITER //\n"
            f"CREATE TRIGGER {table}_owner_repaired AFTER UPDATE ON {table} FOR EACH ROW\n"
            "BEGIN\n"
            "  IF NEW.id=200 THEN SET @sync_owner_repaired=1; END IF;\n"
            "END//\n"
            f"CREATE TRIGGER {table}_retry_failure BEFORE INSERT ON {table} FOR EACH ROW\n"
            "BEGIN\n"
            "  IF COALESCE(@sync_owner_repaired,0)=1 THEN\n"
            "    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected unique-owner retry failure';\n"
            "  END IF;\n"
            "END//\n"
            "DELIMITER ;\n",
        )

        self.reset_target_general_log()
        failed = self.run_sync(tables=[table], run_id=run_id, chunk_size=130)
        failed_sequence = self.sync_unique_owner_insert_sequence(table)
        failed_transaction = self.assert_sync_unique_owner_transaction(
            table,
            owner_action="update",
            outcome="rollback",
        )
        self.admin_sql(self.target, "SET GLOBAL general_log=OFF;")
        failed_output = "\n".join((failed.stdout, failed.stderr))
        if failed.returncode == 0 or "injected unique-owner retry failure" not in failed_output:
            raise HarnessError(
                "unique-owner retry failure was not observed: "
                f"exit={failed.returncode} output={failed_output!r}"
            )
        retained = self.admin_query(
            self.target,
            f"SELECT id,token,page,payload FROM {table} ORDER BY id;",
        ).strip()
        if retained != "200\ttoken-129\tpage-129\tmisfiled-owner":
            raise HarnessError(f"failed sync did not roll back target rows: {retained!r}")
        failed_progress = self.admin_query(
            self.target,
            "SELECT status,IF(last_primary_key_json IS NULL,'<NULL>',last_primary_key_json),"
            "chunks,rows_scanned,inserts_applied,updates_applied,deletes_applied,"
            "IF(last_error IS NULL,'<NULL>',last_error) FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} AND stage='rows' AND table_name={sql_literal(table)};",
        ).strip()
        if failed_progress:
            raise HarnessError(
                f"failed first chunk unexpectedly persisted row-stage progress: {failed_progress!r}"
            )
        retained_progress = self.admin_query(
            self.target,
            "SELECT COUNT(*),SUM(stage='prerequisite_schema' AND status='complete'),"
            "SUM(stage='rows') FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} AND table_name={sql_literal(table)};",
        ).strip()
        if retained_progress != "1\t1\t0":
            raise HarnessError(
                "failed first chunk did not retain resumable prerequisite progress: "
                f"{retained_progress!r}"
            )
        if failed_sequence != ["first", "second", "second"]:
            raise HarnessError(
                f"failed sync strict INSERT sequence was not atomic: {failed_sequence!r}"
            )
        failed_audits = self.sync_unique_owner_audits(failed)
        if failed_audits:
            raise HarnessError(
                "rolled-back unique-owner repair emitted an audit: "
                f"audits={failed_audits!r} progress='<ABSENT>' "
                f"progress={retained_progress!r} insert_sequence={failed_sequence!r} "
                f"transaction={failed_transaction!r}"
            )

        self.admin_sql(
            self.target,
            f"DROP TRIGGER {table}_retry_failure; DROP TRIGGER {table}_owner_repaired;",
        )
        self.reset_target_general_log()
        resumed = self.run_sync(tables=[table], run_id=run_id, chunk_size=130)
        resumed_sequence = self.sync_unique_owner_insert_sequence(table)
        resumed_transaction = self.assert_sync_unique_owner_transaction(
            table,
            owner_action="update",
            outcome="commit",
        )
        self.admin_sql(self.target, "SET GLOBAL general_log=OFF;")
        require_success(resumed, "resumed secondary unique-owner sync")

        source_snapshot = self.admin_query(
            self.source,
            f"SELECT id,token,page,payload FROM {table} ORDER BY id;",
        ).strip()
        target_snapshot = self.admin_query(
            self.target,
            f"SELECT id,token,page,payload FROM {table} ORDER BY id;",
        ).strip()
        if target_snapshot != source_snapshot or len(target_snapshot.splitlines()) != 131:
            raise HarnessError(
                "resumed sync did not converge all 131 source rows: "
                f"source={source_snapshot!r} target={target_snapshot!r}"
            )
        critical_rows = self.admin_query(
            self.target,
            f"SELECT id,token,page,payload FROM {table} WHERE id IN (129,200) ORDER BY id;",
        ).splitlines()
        if critical_rows != [
            "129\ttoken-129\tpage-129\tpayload-129",
            "200\ttoken-200\tpage-200\tpayload-200",
        ]:
            raise HarnessError(f"critical unique-owner rows are wrong: {critical_rows!r}")
        progress = self.admin_query(
            self.target,
            "SELECT status,last_primary_key_json,chunks,rows_scanned,inserts_applied,"
            "updates_applied,deletes_applied FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} AND stage='rows' AND table_name={sql_literal(table)};",
        ).strip()
        if progress != 'complete\t["200"]\t3\t131\t130\t0\t0':
            raise HarnessError(f"resumed unique-owner progress mismatch: {progress!r}")
        if resumed_sequence != ["first", "second", "second"]:
            raise HarnessError(
                f"resumed sync replayed or skipped strict INSERT batches: {resumed_sequence!r}"
            )
        audits = self.sync_unique_owner_audits(resumed)
        if len(audits) != 1:
            raise HarnessError(f"expected one committed reconciliation audit: {audits!r}")
        expected_audit = {
            "event": "sync_unique_owner_reconciliation",
            "table": table,
            "index": "uidx_token_page",
            "action": "update",
            "intended_primary_key": ["129"],
            "owner_primary_key": ["200"],
        }
        if audits[0] != expected_audit:
            raise HarnessError(f"unexpected reconciliation audit: {audits[0]!r}")
        encoded_audit = json.dumps(audits[0], sort_keys=True)
        for secret in ["token-129", "page-129", "payload-129", "misfiled-owner", "token-200"]:
            if secret in encoded_audit:
                raise HarnessError(f"reconciliation audit leaked {secret!r}: {encoded_audit}")
        print(
            "sync_unique_owner_rollback_resume_ok rows=131 chunks=3 inserts=130 "
            "failed_sequence=first,second,second resumed_sequence=first,second,second "
            f"failed_transaction={','.join(failed_transaction)} "
            f"resumed_transaction={','.join(resumed_transaction)} audits=1"
        )

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
        if progress != 'complete	["129"]	129	2':
            raise HarnessError(f"wide sync progress mismatch: {progress!r}")
        print("sync_wide_update_ok rows=129 updates=129 chunks=2")

    def run_sync_bit_values(self) -> None:
        assert self.source and self.target
        table = "sync_bit_values"
        schema = (
            f"DROP TABLE IF EXISTS {table}; "
            f"CREATE TABLE {table} ("
            "id BIGINT NOT NULL PRIMARY KEY, "
            "premium_only BIT(1) NULL, "
            "flags BIT(9) NULL, "
            "mask BIT(64) NULL, "
            "payload VARCHAR(64) NOT NULL"
            ") ENGINE=InnoDB;"
        )
        for endpoint in (self.source, self.target):
            self.admin_sql(endpoint, schema)

        self.admin_sql(
            self.source,
            f"INSERT INTO {table} (id,premium_only,flags,mask,payload) VALUES "
            "(1,b'0',b'000000000',0x0000000000000000,'source-one'),"
            "(3,b'0',b'100000001',0xFFFFFFFFFFFFFFFF,'source-three'),"
            "(4,b'1',b'011111111',0x0100000000000000,'source-four'),"
            "(5,b'1',b'111111111',0x8000000000000000,'source-five'),"
            "(6,NULL,NULL,NULL,'source-six');",
        )
        self.admin_sql(
            self.target,
            f"INSERT INTO {table} (id,premium_only,flags,mask,payload) VALUES "
            "(1,b'0',b'000000000',0x0000000000000000,'target-one'),"
            "(2,b'1',b'000000001',0x0000000000000001,'target-only'),"
            "(3,b'1',b'000000000',0x0000000000000000,'target-three'),"
            "(5,b'0',b'000000000',0x0000000000000000,'target-five'),"
            "(6,b'1',b'000000001',0x0000000000000001,'target-six');",
        )

        run_id = "sync-bit-values"
        select = (
            f"SELECT id,COALESCE(HEX(premium_only),'NULL'),"
            f"COALESCE(CAST(premium_only AS UNSIGNED),'NULL'),"
            f"COALESCE(HEX(flags),'NULL'),COALESCE(CAST(flags AS UNSIGNED),'NULL'),"
            f"COALESCE(HEX(mask),'NULL'),COALESCE(CAST(mask AS UNSIGNED),'NULL'),payload "
            f"FROM {table} ORDER BY id;"
        )
        self.assert_sync_bit_rollback(table, run_id, select)
        result = self.run_sync(tables=[table], run_id=run_id, chunk_size=3)
        require_success(result, "sync BIT values")
        source_rows = self.admin_query(self.source, select).splitlines()
        target_rows = self.admin_query(self.target, select).splitlines()
        if target_rows != source_rows:
            raise HarnessError(
                "sync BIT values did not roundtrip exact hexadecimal/numeric values: "
                f"source={source_rows!r} target={target_rows!r}"
            )
        if len(target_rows) != 5:
            raise HarnessError(f"sync BIT values has wrong row count: {target_rows!r}")
        progress = self.admin_query(
            self.target,
            "SELECT status,last_primary_key_json,chunks,rows_scanned,inserts_applied,"
            "updates_applied,deletes_applied FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} AND stage='rows' "
            f"AND table_name={sql_literal(table)};",
        ).strip()
        if progress != 'complete\t["6"]\t3\t5\t1\t4\t1':
            raise HarnessError(f"sync BIT values progress mismatch: {progress!r}")
        print(
            "sync_bit_values_ok rows=5 updates=4 inserts=1 deletes=1 chunks=3 rollback=true"
        )

    def assert_sync_bit_rollback(self, table: str, run_id: str, select: str) -> None:
        before = self.admin_query(self.target, select)
        self.admin_sql(
            self.target,
            f"CREATE TRIGGER reject_bit_update BEFORE UPDATE ON {table} FOR EACH ROW "
            "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected BIT update rollback';",
        )
        try:
            failed = self.run_sync(tables=[table], run_id=run_id, chunk_size=3)
            error = failed.stdout + failed.stderr
            if failed.returncode == 0 or "injected BIT update rollback" not in error:
                raise HarnessError(
                    f"BIT rollback fixture did not reach its update failure: {error}"
                )
            if self.admin_query(self.target, select) != before:
                raise HarnessError(
                    "failed BIT update did not roll back preceding target-only deletion"
                )
            advanced = self.admin_query(
                self.target,
                "SELECT COUNT(*) FROM cdc.sync_runs "
                f"WHERE run_id={sql_literal(run_id)} AND stage='rows' AND "
                "(last_primary_key_json IS NOT NULL OR chunks<>0 OR rows_scanned<>0 "
                "OR inserts_applied<>0 OR updates_applied<>0 OR deletes_applied<>0);",
            ).strip()
            if advanced != "0":
                raise HarnessError(
                    f"failed BIT update advanced durable row progress: {advanced}"
                )
        finally:
            self.admin_sql(self.target, "DROP TRIGGER reject_bit_update;")

    def stop_sync_process(self, process: subprocess.Popen[str]) -> None:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=30)
        log = getattr(process, "_cdc_log", None)
        if log is not None:
            log.close()

    def sync_row_progress_evidence(self, run_id: str, table: str) -> dict[str, str]:
        assert self.target
        output = self.admin_query(
            self.target,
            "SELECT run_id,stage,table_name,"
            "IF(last_primary_key_json IS NULL,'<NULL>',last_primary_key_json),"
            "chunks,rows_scanned,inserts_applied,updates_applied,deletes_applied,status,"
            "IF(last_error IS NULL,'<NULL>',last_error),"
            "DATE_FORMAT(created_at,'%Y-%m-%d %H:%i:%s.%f'),"
            "DATE_FORMAT(updated_at,'%Y-%m-%d %H:%i:%s.%f'),"
            "IF(completed_at IS NULL,'<NULL>',DATE_FORMAT(completed_at,'%Y-%m-%d %H:%i:%s.%f')) "
            "FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} AND stage='rows' "
            f"AND table_name={sql_literal(table)};",
        ).strip()
        columns = (
            "run_id",
            "stage",
            "table_name",
            "last_primary_key_json",
            "chunks",
            "rows_scanned",
            "inserts_applied",
            "updates_applied",
            "deletes_applied",
            "status",
            "last_error",
            "created_at",
            "updated_at",
            "completed_at",
        )
        fields = output.split("\t")
        if len(fields) != len(columns):
            raise HarnessError(f"unexpected sync row progress evidence for {table}: {output!r}")
        return dict(zip(columns, fields, strict=True))

    def sync_table_state(self, endpoint: Endpoint, table: str) -> tuple[int, int, int]:
        output = self.admin_query(
            endpoint,
            f"SELECT COUNT(*),COUNT(DISTINCT id),"
            f"COALESCE(SUM(CRC32(CONCAT(id,'|',email,'|',payload))),0) FROM {table};",
        ).strip()
        fields = output.split("\t")
        if len(fields) != 3:
            raise HarnessError(f"unexpected sync table state for {table}: {output!r}")
        return tuple(int(field) for field in fields)

    def sync_legacy_run_spec(self, run_id: str, table: str) -> str:
        assert self.target
        return self.admin_query(
            self.target,
            "SELECT run_spec_json FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} AND stage='rows' "
            f"AND table_name={sql_literal(table)};",
        ).strip()

    def wait_for_schema_fixture(
        self, sql: str, expected: str, process: subprocess.Popen[str], log_path: Path
    ) -> str:
        assert self.target
        deadline = time.monotonic() + 30
        while True:
            actual = self.admin_query(self.target, sql).strip()
            if actual == expected:
                return actual
            if process.poll() is not None or time.monotonic() >= deadline:
                raise HarnessError(
                    f"schema fixture boundary missing: expected={expected!r} actual={actual!r} "
                    f"process_exit={process.poll()} log={log_path.read_text()}"
                )
            time.sleep(0.1)

    def start_schema_metadata_blocker(
        self, table: str
    ) -> tuple[subprocess.Popen[str], int]:
        assert self.target
        marker = f"schema_fixture_{table}"
        sleeping_query = f"SELECT SLEEP(240),{sql_literal(marker)}"
        process = self.start_query(
            self.target,
            f"START TRANSACTION; SELECT id FROM `{table}`; {sleeping_query};",
            user="root",
            password=ADMIN_PASSWORD,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            connection = self.admin_query(
                self.target,
                "SELECT PROCESSLIST_ID FROM performance_schema.threads "
                f"WHERE PROCESSLIST_INFO={sql_literal(sleeping_query)};",
            ).strip()
            if connection:
                return process, int(connection)
            if process.poll() is not None:
                break
            time.sleep(0.1)
        process.kill()
        process.wait(timeout=10)
        raise HarnessError(
            f"metadata blocker did not acquire transaction lock on {table}"
        )

    def release_schema_metadata_blocker(
        self, blocker: tuple[subprocess.Popen[str], int]
    ) -> None:
        assert self.target
        process, connection = blocker
        self.admin_sql(self.target, f"KILL CONNECTION {connection};")
        process.wait(timeout=10)
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                stream.close()

    def schema_fixture_constraints(self) -> str:
        assert self.target
        return self.admin_query(
            self.target,
            "SELECT TABLE_NAME,CONSTRAINT_TYPE,CONSTRAINT_NAME "
            "FROM information_schema.TABLE_CONSTRAINTS "
            f"WHERE CONSTRAINT_SCHEMA='{APP_SCHEMA}' "
            "AND TABLE_NAME LIKE 'schema_parallel_%' AND CONSTRAINT_TYPE<>'PRIMARY KEY' "
            "ORDER BY TABLE_NAME,CONSTRAINT_NAME;",
        ).strip()

    def run_guest_range_repair(self, expected: int = 6) -> CommandResult:
        args = self._sync_args(self._sync_binary(), tables=[], run_id="unused")
        args = args[: args.index("--chunk-size")]
        args[1] = "repair-guest-range"
        args.extend(
            [
                "--start-guest-id",
                "100",
                "--end-guest-id",
                "105",
                "--expected-rows",
                str(expected),
                "--batch-size",
                "2",
            ]
        )
        return run(
            args,
            cwd=self.repo,
            env={
                **os.environ,
                "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD,
                "CDC_TARGET_PASSWORD": SYNC_TARGET_PASSWORD,
            },
            timeout=120,
            check=False,
        )

    def run_repair_guest_range(self) -> None:
        assert self.source and self.target
        schema = """
            CREATE TABLE utms (id BIGINT UNSIGNED PRIMARY KEY, label VARCHAR(64))
                DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
            CREATE TABLE guests (
                guest_id BIGINT UNSIGNED NOT NULL,
                guest_hash CHAR(32) NOT NULL,
                utm_id BIGINT UNSIGNED NULL,
                label VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                payload VARBINARY(64) NOT NULL,
                state ENUM('', 'live') NOT NULL,
                created_at DATETIME(6) NOT NULL,
                PRIMARY KEY (guest_id),
                UNIQUE KEY guest_identity (guest_id, guest_hash),
                CONSTRAINT fk_guests_utm_id FOREIGN KEY (utm_id) REFERENCES utms(id)
                    ON DELETE RESTRICT ON UPDATE RESTRICT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
            CREATE TABLE sessions (
                session_id BIGINT UNSIGNED PRIMARY KEY,
                guest_id BIGINT UNSIGNED NOT NULL,
                guest_hash CHAR(32) NOT NULL
            ) ENGINE=InnoDB;
            INSERT INTO utms VALUES (7,'campaign');
        """
        self.admin_sql(self.source, schema)
        self.admin_sql(
            self.target,
            schema.replace(
                "CONSTRAINT fk_guests_utm_id", "CONSTRAINT guests_fk_guests_utm_id"
            ),
        )
        rows = [
            f"({key},'{key:032x}',{'NULL' if key == 105 else '7'},"
            f"'guest-é-{key}',X'00FF80{key:02x}',"
            f"'{'' if key % 2 else 'live'}','2026-01-02 03:04:05.123456')"
            for key in range(99, 107)
        ]
        self.admin_sql(self.source, "INSERT INTO guests VALUES " + ",".join(rows) + ";")
        self.admin_sql(
            self.target,
            "INSERT INTO sessions VALUES "
            + ",".join(f"({key + 1000},{key},'{key:032x}')" for key in range(100, 106))
            + ";",
        )
        self.write_checkpoint(self.coordinate())
        self.admin_sql(
            self.target,
            "CREATE TABLE cdc.guest_repair_sentinel (id INT PRIMARY KEY,payload VARBINARY(64));"
            "INSERT INTO cdc.guest_repair_sentinel VALUES (1,'guest-repair-unchanged');",
        )
        snapshot_sql = (
            "SELECT guest_id,guest_hash,utm_id,HEX(label),HEX(payload),state+0,"
            "created_at FROM guests ORDER BY guest_id,guest_hash;"
        )
        source_before = self.admin_query(self.source, snapshot_sql)
        sessions_before = self.admin_query(
            self.target, "SELECT * FROM sessions ORDER BY session_id;"
        )
        utms_before = self.admin_query(self.target, "SELECT * FROM utms ORDER BY id;")
        control_tables = self.admin_query(
            self.target,
            "SELECT table_name FROM information_schema.tables WHERE table_schema='cdc' ORDER BY table_name;",
        ).splitlines()
        control_before = {
            table: sorted(
                self.admin_query(
                    self.target, f"SELECT * FROM cdc.`{table}`;"
                ).splitlines()
            )
            for table in control_tables
        }

        def invoke(expected: int = 6) -> CommandResult:
            result = self.run_guest_range_repair(expected)
            tables_after = self.admin_query(
                self.target,
                "SELECT table_name FROM information_schema.tables WHERE table_schema='cdc' ORDER BY table_name;",
            ).splitlines()
            if tables_after != control_tables:
                raise HarnessError("guest repair changed CDC table inventory")
            for table, before in control_before.items():
                after = sorted(
                    self.admin_query(
                        self.target, f"SELECT * FROM cdc.`{table}`;"
                    ).splitlines()
                )
                if after != before:
                    raise HarnessError(f"guest repair changed CDC table {table}")
            if self.admin_query(self.source, snapshot_sql) != source_before:
                raise HarnessError("guest repair changed source rows")
            if (
                self.admin_query(
                    self.target, "SELECT * FROM sessions ORDER BY session_id;"
                )
                != sessions_before
            ):
                raise HarnessError("guest repair changed existing sessions")
            return result

        def refuse_without_writes(expected: int, reason: str) -> None:
            before = self.admin_query(self.target, snapshot_sql)
            result = invoke(expected)
            if result.returncode == 0:
                raise HarnessError(f"guest repair accepted {reason}")
            if self.admin_query(self.target, snapshot_sql) != before:
                raise HarnessError(f"guest repair wrote rows despite {reason}")

        expected_rows = self.admin_query(
            self.source,
            snapshot_sql.replace(
                "FROM guests ORDER",
                "FROM guests WHERE guest_id BETWEEN 100 AND 105 ORDER",
            ),
        )
        require_success(invoke(), "guest range full-row repair")
        if self.admin_query(self.target, snapshot_sql) != expected_rows:
            raise HarnessError(
                "guest range repair did not preserve exact full values/range"
            )
        # Existing equal rows must not execute UPDATE or replacement DELETE/INSERT.
        for event in ("UPDATE", "DELETE", "INSERT"):
            self.admin_sql(
                self.target,
                f"CREATE TRIGGER guest_no_{event.lower()} BEFORE {event} ON guests "
                "FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='equal rows must be no-op';",
            )
        require_success(invoke(), "guest range equal-row no-op")
        for event in ("update", "delete", "insert"):
            self.admin_sql(self.target, f"DROP TRIGGER guest_no_{event};")
        self.admin_sql(self.target, "DELETE FROM guests;")
        refuse_without_writes(7, "source count mismatch")
        self.admin_sql(
            self.target,
            "INSERT INTO guests VALUES "
            + rows[1]
            + "; UPDATE guests SET payload=X'AA' WHERE guest_id=100;",
        )
        refuse_without_writes(6, "existing target full-row mismatch")
        self.admin_sql(self.target, "DELETE FROM guests; DELETE FROM utms;")
        refuse_without_writes(6, "missing target UTM parent")
        if self.admin_query(self.target, "SELECT COUNT(*) FROM utms;").strip() != "0":
            raise HarnessError("guest repair unexpectedly inserted missing UTM")
        self.admin_sql(self.target, "INSERT INTO utms VALUES (7,'campaign');")
        self.admin_sql(
            self.target,
            "DELIMITER //\n"
            "CREATE TRIGGER guest_fail_second_batch BEFORE INSERT ON guests FOR EACH ROW "
            "BEGIN IF NEW.guest_id=103 THEN SIGNAL SQLSTATE '45000' "
            "SET MESSAGE_TEXT='guest second batch failure'; END IF; END//\n"
            "DELIMITER ;\n",
        )
        failed = invoke()
        if failed.returncode == 0:
            raise HarnessError("guest repair ignored second-batch failure")
        first_batch = self.admin_query(
            self.source,
            snapshot_sql.replace(
                "FROM guests ORDER",
                "FROM guests WHERE guest_id BETWEEN 100 AND 101 ORDER",
            ),
        )
        if self.admin_query(self.target, snapshot_sql) != first_batch:
            raise HarnessError(
                "guest failure did not preserve batch one and roll back batch two"
            )
        self.admin_sql(
            self.target,
            "DROP TRIGGER guest_fail_second_batch;\n"
            "DELIMITER //\n"
            "CREATE TRIGGER guest_preserve_first BEFORE INSERT ON guests FOR EACH ROW "
            "BEGIN IF NEW.guest_id<102 THEN SIGNAL SQLSTATE '45000' "
            "SET MESSAGE_TEXT='committed rows must be no-op'; END IF; END//\n"
            "DELIMITER ;\n",
        )
        require_success(invoke(), "guest range rerun after committed batch")
        if self.admin_query(self.target, snapshot_sql) != expected_rows:
            raise HarnessError("guest rerun did not preserve/finish exact source rows")
        if (
            self.admin_query(self.target, "SELECT * FROM utms ORDER BY id;")
            != utms_before
        ):
            raise HarnessError("guest repair changed UTM parents")
        print(
            "guest_range_ok rows=6 batch_size=2 full_values=exact equal=no-op "
            "mismatch=refused count=refused missing_utm=refused "
            "partial_batch=rollback rerun=complete cdc=unchanged"
        )

    def run_repair_fk_case(self, case: str, expected: int) -> CommandResult:
        args = self._sync_args(self._sync_binary(), tables=[], run_id="unused")
        args = args[: args.index("--chunk-size")]
        args[1] = "repair-fk-orphans"
        args.extend(
            ["--case", case, "--expected-orphans", str(expected), "--batch-size", "50"]
        )
        return run(
            args,
            cwd=self.repo,
            env={
                **os.environ,
                "CDC_SOURCE_PASSWORD": SOURCE_PASSWORD,
                "CDC_TARGET_PASSWORD": SYNC_TARGET_PASSWORD,
            },
            timeout=120,
            check=False,
        )

    def run_repair_fk_orphans_parents(self) -> None:
        assert self.source and self.target
        self.admin_sql(
            self.target,
            "CREATE TABLE cdc.repair_control_sentinel (id INT PRIMARY KEY, payload VARBINARY(64)); INSERT INTO cdc.repair_control_sentinel VALUES (1,X'00FF1234');",
        )
        control_tables = self.admin_query(
            self.target,
            "SELECT table_name FROM information_schema.tables WHERE table_schema='cdc' AND table_name<>'repair_control_sentinel' ORDER BY table_name;",
        ).splitlines()
        control_before = {
            table: self.admin_query(self.target, f"SELECT * FROM cdc.`{table}`;")
            for table in control_tables
        }
        cases = [
            (
                "comics-langs-category",
                "comics_langs",
                "ibfk_accl_category",
                "comic_category_id",
                "section_id",
            ),
            (
                "comics-langs-type",
                "comics_langs",
                "ibfk_accl_type",
                "comic_type_id",
                "comic_type_id",
            ),
            ("releases-name", "releases", "releases_ibfk_1", "comic_name", "name"),
            (
                "releases-type",
                "releases",
                "releases_ibfk_10",
                "comic_type_id",
                "comic_type_id",
            ),
            (
                "releases-category",
                "releases",
                "releases_ibfk_2",
                "comic_category_id",
                "section_id",
            ),
            (
                "releases-visibility",
                "releases",
                "releases_ibfk_3",
                "comic_is_visible",
                "is_visible",
            ),
            ("releases-id", "releases", "releases_ibfk_6", None, None),
            ("releases-slug", "releases", "releases_ibfk_7", "comic_slug", "slug"),
            (
                "releases-show-in-list",
                "releases",
                "releases_ibfk_9",
                "comic_show_in_list",
                "show_in_list",
            ),
            (
                "releases-format",
                "releases",
                "releases_ibfk_format",
                "comic_format_id",
                "comic_format_id",
            ),
        ]
        for case, child, constraint, child_col, parent_col in cases:
            self.repair_parent_fixture(case, child, constraint, child_col, parent_col)
        self.repair_artists_favorites_fixture()
        for table, expected in control_before.items():
            if (
                self.admin_query(self.target, f"SELECT * FROM cdc.`{table}`;")
                != expected
            ):
                raise HarnessError(f"repair changed CDC control-plane table {table}")
        sentinel = self.admin_query(
            self.target, "SELECT id,HEX(payload) FROM cdc.repair_control_sentinel;"
        ).strip()
        if sentinel != "1\t00FF1234":
            raise HarnessError(f"repair changed control-plane sentinel: {sentinel!r}")

    def repair_parent_fixture(
        self,
        case: str,
        child: str,
        constraint: str,
        child_col: str | None,
        parent_col: str | None,
    ) -> None:
        assert self.source and self.target
        parent_extra = f", `{parent_col}` VARCHAR(32) NOT NULL" if parent_col else ""
        child_extra = f", `{child_col}` VARCHAR(32) NOT NULL" if child_col else ""
        slug = case == "releases-slug"
        parent_key = (
            f"`{parent_col}`"
            if slug
            else "id" + (f",`{parent_col}`" if parent_col else "")
        )
        child_key = (
            f"`{child_col}`"
            if slug
            else "comic_id" + (f",`{child_col}`" if child_col else "")
        )
        ddl = (
            "SET FOREIGN_KEY_CHECKS=0; DROP TABLE IF EXISTS releases,comics_langs,comics,artists; SET FOREIGN_KEY_CHECKS=1;"
            "CREATE TABLE artists (id BIGINT PRIMARY KEY,name VARCHAR(32) NOT NULL,payload MEDIUMBLOB NOT NULL,state ENUM('','live') NOT NULL,UNIQUE KEY artist_key(id,name));"
            f"CREATE TABLE comics (id BIGINT PRIMARY KEY{parent_extra}, payload MEDIUMBLOB NOT NULL, state ENUM('','live') NOT NULL,artist_id BIGINT NOT NULL,artist_name VARCHAR(32) NOT NULL, UNIQUE KEY parent_key ({parent_key}),"
            "CONSTRAINT comics_ibfk_5 FOREIGN KEY(artist_id,artist_name) REFERENCES artists(id,name) ON UPDATE CASCADE ON DELETE RESTRICT);"
            f"CREATE TABLE `{child}` (id BIGINT PRIMARY KEY,comic_id BIGINT NOT NULL{child_extra},"
            f"CONSTRAINT `{constraint}` FOREIGN KEY ({child_key}) REFERENCES comics ({parent_key}) ON UPDATE CASCADE ON DELETE RESTRICT);"
        )
        self.admin_sql(self.source, ddl)
        fk_clause = f"CONSTRAINT `{constraint}` FOREIGN KEY ({child_key}) REFERENCES comics ({parent_key}) ON UPDATE CASCADE ON DELETE RESTRICT"
        self.admin_sql(self.target, ddl.replace("," + fk_clause, ""))
        values = lambda key, value: (
            f"({key}"
            + (f",'{value}'" if parent_col else "")
            + f",X'00FF80','live',{key + 10},'artist{key}')"
        )
        artist_missing = "(11,'artist1',X'00FE81','')"
        artist_existing = "(12,'artist2',X'FF0082','live')"
        self.admin_sql(
            self.source,
            f"INSERT INTO artists VALUES {artist_missing},{artist_existing};",
        )
        self.admin_sql(self.target, f"INSERT INTO artists VALUES {artist_existing};")
        self.admin_sql(
            self.source,
            "INSERT INTO comics VALUES "
            + values(1, "fresh1")
            + ","
            + values(2, "fresh2")
            + ";",
        )

        def row(key: int, comic: int, value: str) -> str:
            return f"({key},{comic}" + (f",'{value}'" if child_col else "") + ")"

        self.admin_sql(
            self.source,
            f"INSERT INTO `{child}` VALUES {row(1, 1, 'fresh1')},{row(2, 2, 'fresh2')};",
        )
        self.admin_sql(
            self.target,
            "INSERT INTO comics VALUES "
            + values(2, "stale")
            + "; SET FOREIGN_KEY_CHECKS=0;"
            f"INSERT INTO `{child}` VALUES {row(1, 999, 'orphan1')},{row(2, 998, 'orphan2')},{row(3, 997, 'orphan3')}; SET FOREIGN_KEY_CHECKS=1;",
        )
        snapshot_sql = (
            f"SELECT * FROM `{child}` ORDER BY id; SELECT id"
            + (f",`{parent_col}`" if parent_col else "")
            + ",HEX(payload),state+0,artist_id,artist_name FROM comics ORDER BY id;"
            "SELECT id,name,HEX(payload),state+0 FROM artists ORDER BY id;"
        )
        source_before = self.admin_query(self.source, snapshot_sql)
        self.admin_sql(
            self.target,
            "INSERT INTO artists VALUES (11,'wrong-artist',X'AA','live');",
        )
        mismatch_before = self.admin_query(self.target, snapshot_sql)
        mismatch = self.run_repair_fk_case(case, 3)
        if mismatch.returncode == 0:
            raise HarnessError(f"{case} accepted existing artist key mismatch")
        if self.admin_query(self.target, snapshot_sql) != mismatch_before:
            raise HarnessError(f"{case} mutated rows on artist key mismatch refusal")
        self.admin_sql(self.target, "DELETE FROM artists WHERE id=11;")
        before = self.admin_query(self.target, snapshot_sql)
        self.admin_sql(
            self.target,
            f"DELIMITER $$\nCREATE TRIGGER repair_fail BEFORE UPDATE ON `{child}` FOR EACH ROW BEGIN "
            "IF NEW.id=2 THEN "
            "IF NOT EXISTS(SELECT 1 FROM artists WHERE id=11 AND name='artist1' AND payload=X'00FE81' AND state+0=1) "
            "OR NOT EXISTS(SELECT 1 FROM comics WHERE id=1 AND artist_id=11 AND artist_name='artist1') "
            f"OR NOT EXISTS(SELECT 1 FROM `{child}` WHERE id=1 AND comic_id=1) THEN "
            "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='repair ancestor ordering missing'; END IF; "
            "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='repair child write blocked'; END IF; END$$\nDELIMITER ;\n",
        )
        failed = self.run_repair_fk_case(case, 3)
        if failed.returncode == 0 or "repair child write blocked" not in failed.stderr:
            raise HarnessError(
                f"{case} did not reach post-parent rollback boundary: {failed}"
            )
        if self.admin_query(self.target, snapshot_sql) != before:
            raise HarnessError(
                f"{case} leaked artist/parent/child mutation after rollback"
            )
        self.admin_sql(self.target, "DROP TRIGGER repair_fail;")
        repaired = self.run_repair_fk_case(case, 3)
        require_success(repaired, case)
        if (
            "planned=3 examined=3 updated=2 deleted=1" not in repaired.stdout
            or "remaining=0" not in repaired.stdout
        ):
            raise HarnessError(f"{case} wrong repair counts: {repaired.stdout}")
        if (
            self.admin_query(self.target, snapshot_sql) != source_before
            or self.admin_query(self.source, snapshot_sql) != source_before
        ):
            raise HarnessError(f"{case} source/target exact row fidelity mismatch")
        require_success(self.run_repair_fk_case(case, 0), case + " idempotence")
        if self.admin_query(self.target, snapshot_sql) != source_before:
            raise HarnessError(f"{case} idempotent rerun changed exact rows")
        self.admin_sql(self.target, f"ALTER TABLE `{child}` ADD {fk_clause};")
        if parent_col:
            self.admin_sql(
                self.target,
                f"UPDATE comics SET `{parent_col}`='cascade-proof' WHERE id=1;",
            )
            cascaded = self.admin_query(
                self.target, f"SELECT `{child_col}` FROM `{child}` WHERE id=1;"
            ).strip()
            if cascaded != "cascade-proof":
                raise HarnessError(
                    f"{case} ON UPDATE CASCADE not enforced: {cascaded!r}"
                )
        self.assert_admin_sql_rejected(
            self.target, "DELETE FROM comics WHERE id=1;", "1451"
        )
        self.assert_admin_sql_rejected(
            self.target,
            f"INSERT INTO `{child}` VALUES {row(99, 999, 'missing')};",
            "1452",
        )
        print(
            f"repair_parent_case_pass case={case} updated=2 deleted=1 artist_comic_child_order=true rollback=exact idempotent=true artist_key_mismatch=refused"
        )

    def repair_artists_favorites_fixture(self) -> None:
        assert self.source and self.target
        ddl = (
            "CREATE TABLE users (id BIGINT PRIMARY KEY,name VARCHAR(32) NOT NULL,UNIQUE KEY user_name(id,name));"
            "CREATE TABLE artists_favorites(id BIGINT PRIMARY KEY,user_id BIGINT NOT NULL,user_username VARCHAR(32) NOT NULL,"
            "CONSTRAINT artists_favorites_ibfk_2 FOREIGN KEY(user_id,user_username) REFERENCES users(id,name) ON UPDATE CASCADE ON DELETE RESTRICT);"
        )
        self.admin_sql(self.source, ddl + "INSERT INTO users VALUES(1,'fresh');")
        fk_clause = "CONSTRAINT artists_favorites_ibfk_2 FOREIGN KEY(user_id,user_username) REFERENCES users(id,name) ON UPDATE CASCADE ON DELETE RESTRICT"
        self.admin_sql(
            self.target,
            ddl.replace("," + fk_clause, "") + "INSERT INTO users VALUES(1,'fresh');",
        )
        self.admin_sql(
            self.source,
            "INSERT INTO artists_favorites VALUES "
            + ",".join(f"({i},1,'fresh')" for i in range(1, 8))
            + ";",
        )
        self.admin_sql(
            self.target,
            "SET FOREIGN_KEY_CHECKS=0; INSERT INTO artists_favorites VALUES "
            + ",".join(f"({i},1,'stale')" for i in range(1, 30))
            + "; SET FOREIGN_KEY_CHECKS=1;",
        )
        result = self.run_repair_fk_case("artists-favorites", 29)
        require_success(result, "artists-favorites")
        if "planned=29 examined=29 updated=7 deleted=22" not in result.stdout:
            raise HarnessError(f"artists-favorites counts: {result.stdout}")
        for table in ("users", "artists_favorites"):
            if self.admin_query(
                self.target, f"SELECT * FROM {table} ORDER BY id;"
            ) != self.admin_query(self.source, f"SELECT * FROM {table} ORDER BY id;"):
                raise HarnessError(f"artists-favorites parity {table}")
        require_success(
            self.run_repair_fk_case("artists-favorites", 0),
            "artists-favorites idempotence",
        )

    def run_sync_schema_parallel_resume(self) -> None:
        assert self.source and self.target
        run_id = "sync-schema-parallel-resume"
        child, parent, independent = (
            "schema_parallel_a_child",
            "schema_parallel_b_parent",
            "schema_parallel_c_independent",
        )
        tables = [child, parent, independent]
        for endpoint in (self.source, self.target):
            for table in (parent, independent, child):
                self.admin_sql(
                    endpoint,
                    f"CREATE TABLE `{table}` (id INT PRIMARY KEY, parent_id INT NOT NULL, "
                    "value INT NOT NULL, KEY parent_idx(parent_id)) ENGINE=InnoDB;",
                )
        for table in tables:
            self.admin_sql(
                self.source,
                f"INSERT INTO `{table}` VALUES (1,1,10),(2,2,20); "
                f"ALTER TABLE `{table}` ADD CONSTRAINT `{table}_positive` CHECK(value>0);",
            )
        self.admin_sql(
            self.source,
            f"ALTER TABLE `{parent}` ADD CONSTRAINT `{parent}_upper` CHECK(value<100); "
            f"ALTER TABLE `{child}` ADD CONSTRAINT `{child}_parent_fk` "
            f"FOREIGN KEY(parent_id) REFERENCES `{parent}`(id);",
        )
        # Row workers use LOCK TABLES WRITE, incompatible with our MDL holders.
        # Stop at the final-stage entry first, without editing persisted progress.
        self.admin_sql(
            self.target,
            "DELIMITER //\n"
            "CREATE TRIGGER cdc.pause_schema_fixture BEFORE INSERT ON cdc.sync_runs "
            "FOR EACH ROW BEGIN IF NEW.stage='final_constraints' THEN "
            "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='schema fixture final entry'; "
            "END IF; END//\nDELIMITER ;\n",
        )
        try:
            seeded = self.run_sync(
                tables=tables, run_id=run_id, chunk_size=1, parallelism=2
            )
            if (
                seeded.returncode == 0
                or "schema fixture final entry" not in seeded.stdout + seeded.stderr
            ):
                raise HarnessError(f"final-stage entry fixture failed: {seeded}")
        finally:
            self.admin_sql(self.target, "DROP TRIGGER cdc.pause_schema_fixture;")
        self.reset_target_general_log()
        blockers = {}
        process = None
        pending_sql = (
            "SELECT GROUP_CONCAT(DISTINCT m.OBJECT_NAME ORDER BY m.OBJECT_NAME) "
            "FROM performance_schema.metadata_locks m "
            "JOIN performance_schema.threads t ON t.THREAD_ID=m.OWNER_THREAD_ID "
            f"WHERE t.PROCESSLIST_USER='{SYNC_TARGET_USER}' "
            "AND t.PROCESSLIST_INFO LIKE 'ALTER TABLE%' AND m.LOCK_STATUS='PENDING' "
            f"AND m.OBJECT_SCHEMA='{APP_SCHEMA}';"
        )
        try:
            for table in tables:
                blockers[table] = self.start_schema_metadata_blocker(table)
            process, log_path = self.start_sync(
                tables=tables, run_id=run_id, chunk_size=1, parallelism=2
            )
            self.wait_for_schema_fixture(
                pending_sql, f"{parent},{independent}", process, log_path
            )
            sessions = (
                self.admin_query(
                    self.target,
                    "SELECT PROCESSLIST_ID,PROCESSLIST_STATE FROM performance_schema.threads "
                    f"WHERE PROCESSLIST_USER='{SYNC_TARGET_USER}' "
                    "AND PROCESSLIST_INFO LIKE 'ALTER TABLE%' ORDER BY PROCESSLIST_ID;",
                )
                .strip()
                .splitlines()
            )
            if len(sessions) != 2 or any(
                "metadata lock" not in row for row in sessions
            ):
                raise HarnessError(
                    f"expected exactly two metadata-blocked ALTER sessions: {sessions}"
                )
            print(f"schema_parallel_overlap parallelism=2 sessions={sessions!r}")
            row_before = {
                table: self.sync_row_progress_evidence(run_id, table)
                for table in tables
            }
            if any(row["status"] != "complete" for row in row_before.values()):
                raise HarnessError(
                    f"ALTER overlap occurred before all row stages completed: {row_before}"
                )

            paused_connection = int(
                self.admin_query(
                    self.target,
                    "SELECT PROCESSLIST_ID FROM performance_schema.threads "
                    f"WHERE PROCESSLIST_USER='{SYNC_TARGET_USER}' "
                    f"AND PROCESSLIST_INFO LIKE 'ALTER TABLE `{independent}`%';",
                ).strip()
            )
            process.send_signal(signal.SIGSTOP)
            pause_deadline = time.monotonic() + 10
            while True:
                client_status = Path(f"/proc/{process.pid}/status").read_text()
                client_state = next(
                    line
                    for line in client_status.splitlines()
                    if line.startswith("State:")
                )
                if "T (stopped)" in client_state:
                    break
                if time.monotonic() >= pause_deadline:
                    raise HarnessError(f"client did not stop: {client_state}")
                time.sleep(0.05)
            self.release_schema_metadata_blocker(blockers.pop(independent))
            independent_constraint = f"{independent}\tCHECK\t{independent}_positive"
            constraint_count_sql = (
                "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS "
                f"WHERE CONSTRAINT_SCHEMA='{APP_SCHEMA}' AND TABLE_NAME='{independent}' "
                "AND CONSTRAINT_TYPE='CHECK';"
            )
            self.wait_for_schema_fixture(constraint_count_sql, "1", process, log_path)
            self.wait_for_schema_fixture(
                "SELECT PROCESSLIST_COMMAND FROM performance_schema.threads "
                f"WHERE PROCESSLIST_ID={paused_connection};",
                "Sleep",
                process,
                log_path,
            )
            client_state = next(
                line
                for line in Path(f"/proc/{process.pid}/status").read_text().splitlines()
                if line.startswith("State:")
            )
            if "T (stopped)" not in client_state:
                raise HarnessError(f"client resumed unexpectedly: {client_state}")
            print(
                f"schema_paused_handoff pid={process.pid} client_state={client_state!r} "
                f"connection={paused_connection} server_command=Sleep constraint={independent_constraint!r}"
            )
            self.wait_for_schema_fixture(pending_sql, parent, process, log_path)
            # A free worker must not launch the child before its blocked parent finishes.
            child_submissions = self.admin_query(
                self.target,
                "SELECT COUNT(*) FROM mysql.general_log "
                f"WHERE user_host LIKE '{SYNC_TARGET_USER}%' "
                f"AND argument LIKE 'ALTER TABLE `{child}`%';",
            ).strip()
            if (
                child_submissions != "0"
                or self.schema_fixture_constraints() != independent_constraint
            ):
                raise HarnessError("dependent child ran before its parent converged")
            self.stop_sync_process(process)
            process = None
            # SIGKILL closes the client; explicitly terminate its blocked server statement
            # before resume, rather than letting a disconnected DDL finish after lock release.
            remaining = self.admin_query(
                self.target,
                "SELECT PROCESSLIST_ID FROM performance_schema.threads "
                f"WHERE PROCESSLIST_USER='{SYNC_TARGET_USER}' AND PROCESSLIST_INFO LIKE 'ALTER TABLE%';",
            ).splitlines()
            for connection in remaining:
                self.admin_sql(self.target, f"KILL CONNECTION {int(connection)};")
            stage_before = self.admin_query(
                self.target,
                "SELECT stage,status,COUNT(*) FROM cdc.sync_runs "
                f"WHERE run_id='{run_id}' GROUP BY stage,status ORDER BY stage,status;",
            ).strip()
            if "final_constraints\trunning\t3" not in stage_before:
                raise HarnessError(
                    f"did not interrupt the final schema stage: {stage_before!r}"
                )
            print(
                f"schema_parallel_interrupted stages={stage_before!r} constraints={independent_constraint!r}"
            )

            process, log_path = self.start_sync(
                tables=tables, run_id=run_id, chunk_size=1, parallelism=2
            )
            self.wait_for_schema_fixture(pending_sql, parent, process, log_path)
            self.release_schema_metadata_blocker(blockers.pop(parent))
            self.wait_for_schema_fixture(pending_sql, child, process, log_path)
            parent_constraints = self.schema_fixture_constraints().splitlines()
            expected_partial = [
                f"{parent}\tCHECK\t{parent}_positive",
                f"{parent}\tCHECK\t{parent}_upper",
                independent_constraint,
            ]
            if parent_constraints != expected_partial:
                raise HarnessError(
                    f"child started before all parent statements completed: {parent_constraints}"
                )
            self.release_schema_metadata_blocker(blockers.pop(child))
            process.wait(timeout=90)
            if process.returncode:
                raise HarnessError(
                    f"schema-stage resume failed: {log_path.read_text()}"
                )
            self.stop_sync_process(process)
            process = None
            self.assert_schema_parallel_resumed(tables, run_id, row_before)
        finally:
            if process is not None:
                self.stop_sync_process(process)
            for blocker in blockers.values():
                self.release_schema_metadata_blocker(blocker)

    def assert_schema_parallel_resumed(
        self, tables: list[str], run_id: str, row_before: dict[str, dict[str, str]]
    ) -> None:
        assert self.source and self.target
        child, parent, independent = tables
        expected = sorted(
            [
                f"{child}\tFOREIGN KEY\t{child}_parent_fk",
                f"{child}\tCHECK\t{child}_positive",
                f"{parent}\tCHECK\t{parent}_positive",
                f"{parent}\tCHECK\t{parent}_upper",
                f"{independent}\tCHECK\t{independent}_positive",
            ],
            key=lambda row: (row.split("\t")[0], row.split("\t")[2]),
        )
        if self.schema_fixture_constraints().splitlines() != expected:
            raise HarnessError(
                f"resume constraints differ: {self.schema_fixture_constraints()!r}"
            )
        for table in tables:
            for endpoint in (self.source, self.target):
                data = self.admin_query(
                    endpoint, f"SELECT id,parent_id,value FROM `{table}` ORDER BY id;"
                ).strip()
                if data != "1\t1\t10\n2\t2\t20":
                    raise HarnessError(f"schema resume corrupted {table}: {data!r}")
            if self.sync_row_progress_evidence(run_id, table) != row_before[table]:
                raise HarnessError(
                    f"schema resume replayed completed row progress for {table}"
                )
            self.assert_admin_sql_rejected(
                self.target, f"INSERT INTO `{table}` VALUES (3,1,-1);", "3819"
            )
        self.assert_admin_sql_rejected(
            self.target, f"INSERT INTO `{parent}` VALUES (3,1,100);", "3819"
        )
        self.assert_admin_sql_rejected(
            self.target, f"INSERT INTO `{child}` VALUES (3,999,10);", "1452"
        )
        stages = self.admin_query(
            self.target,
            "SELECT stage,status,COUNT(*) FROM cdc.sync_runs "
            f"WHERE run_id='{run_id}' GROUP BY stage,status ORDER BY stage,status;",
        ).strip()
        expected_stages = "final_constraints\tcomplete\t3\nprerequisite_schema\tcomplete\t3\nrows\tcomplete\t3"
        if stages != expected_stages:
            raise HarnessError(f"schema resume stages not complete: {stages!r}")
        independent_alters = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM mysql.general_log "
            f"WHERE user_host LIKE '{SYNC_TARGET_USER}%' "
            "AND command_type IN ('Query','Execute') "
            f"AND argument LIKE 'ALTER TABLE `{independent}`%';",
        ).strip()
        if independent_alters != "1":
            raise HarnessError(
                f"resume repeated already-applied independent DDL: {independent_alters}"
            )
        self.assert_schema_fixture_statement_order(tables)
        print(
            "sync_schema_parallel_resume_ok overlap=2 dependency_order=true per_table_order=true "
            "same_run=true completed_rows_preserved=true committed_ddl_preserved=true "
            "constraints=5 rows=6 check_and_fk_enforced=true"
        )

    def assert_schema_fixture_statement_order(self, tables: list[str]) -> None:
        assert self.target
        child, parent, independent = tables
        expected = {
            child: [f"{child}_positive", f"{child}_parent_fk"],
            parent: [f"{parent}_positive", f"{parent}_positive", f"{parent}_upper"],
            independent: [f"{independent}_positive"],
        }
        # Observe executed database commands, not Prepare records or generated plans.
        # The first parent attempt was killed while waiting; resume must retry it
        # before applying the parent's second constraint.
        records = self.admin_query(
            self.target,
            "SELECT argument FROM mysql.general_log "
            f"WHERE user_host LIKE '{SYNC_TARGET_USER}%' "
            "AND command_type IN ('Query','Execute') AND argument LIKE 'ALTER TABLE%' "
            "ORDER BY event_time;",
        ).splitlines()
        observed = {table: [] for table in tables}
        for statement in records:
            for table in tables:
                names = {name for name in expected[table] if f"`{name}`" in statement}
                if len(names) == 1:
                    observed[table].append(names.pop())
        if observed != expected:
            raise HarnessError(
                f"per-table ALTER execution order mismatch: {observed!r}; records={records!r}"
            )
        print(f"schema_parallel_statement_order observed={observed!r}")

    def run_sync_legacy_complete_resume(self) -> None:
        assert self.source and self.target
        run_id = "sync-legacy-complete-resume"
        tables = ["legacy_parent", "legacy_child"]
        for endpoint, row_id, label in (
            (self.source, 1, "source"),
            (self.target, 2, "target"),
        ):
            self.admin_sql(
                endpoint,
                "CREATE TABLE legacy_parent (id INT PRIMARY KEY, label VARCHAR(32) NOT NULL) "
                "ENGINE=InnoDB; "
                "CREATE TABLE legacy_child (id INT PRIMARY KEY, parent_id INT NOT NULL, "
                "label VARCHAR(32) NOT NULL, CONSTRAINT legacy_child_parent "
                "FOREIGN KEY (parent_id) REFERENCES legacy_parent(id), "
                "CONSTRAINT legacy_child_positive CHECK (id > 0)) ENGINE=InnoDB; "
                f"INSERT INTO legacy_parent VALUES ({row_id},'{label}-parent'); "
                f"INSERT INTO legacy_child VALUES ({row_id},{row_id},'{label}-child');",
            )
        for index, table in enumerate(tables, start=1):
            for stage in ("prerequisite_schema", "rows"):
                self.admin_sql(
                    self.target,
                    "INSERT INTO cdc.sync_runs "
                    "(run_id,stage,table_name,run_spec_json,last_primary_key_json,"
                    "chunks,rows_scanned,inserts_applied,updates_applied,deletes_applied,"
                    "status,last_error,created_at,updated_at,completed_at) VALUES "
                    f"({sql_literal(run_id)},{sql_literal(stage)},{sql_literal(table)},"
                    f"'{json.dumps({'legacy': table, 'stage': stage})}','[{100 + index}]',"
                    f"{index + 10},{index + 1000},{index + 20},{index + 30},{index + 40},"
                    "'complete',NULL,'2026-07-01 01:02:03.123456',"
                    "'2026-07-02 02:03:04.234567','2026-07-02 02:03:04.234567');",
                )
        self.admin_sql(
            self.target,
            "DROP TABLE cdc.sync_runs_phases; "
            f"REVOKE SELECT, INSERT, UPDATE ON cdc.sync_runs_phases FROM '{SYNC_TARGET_USER}'@'%';",
        )
        legacy_query = (
            "SELECT * FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} "
            "AND stage IN ('prerequisite_schema','rows') ORDER BY stage,table_name;"
        )
        legacy_before = self.admin_query(self.target, legacy_query)
        if len(legacy_before.splitlines()) != 4:
            raise HarnessError(
                f"legacy fixture must contain four full records: {legacy_before!r}"
            )
        data_before = {
            (endpoint.container, table): self.admin_query(
                endpoint, f"SELECT * FROM `{table}` ORDER BY id;"
            )
            for endpoint in (self.source, self.target)
            for table in tables
        }
        for table in tables:
            if (
                data_before[(self.source.container, table)]
                == data_before[(self.target.container, table)]
            ):
                raise HarnessError(f"legacy fixture must deliberately differ: {table}")
        for endpoint in (self.source, self.target):
            self.admin_sql(
                endpoint,
                "SET GLOBAL general_log=OFF; TRUNCATE TABLE mysql.general_log; "
                "SET GLOBAL log_output='TABLE'; SET GLOBAL general_log=ON;",
            )
        try:
            result = self.run_sync(
                tables=tables, run_id=run_id, parallelism=2, timeout=90
            )
        finally:
            for endpoint in (self.source, self.target):
                self.admin_sql(endpoint, "SET GLOBAL general_log=OFF;")
        print(f"legacy_complete_sync_stdout:\n{result.stdout}")
        print(f"legacy_complete_sync_stderr:\n{result.stderr}")
        require_success(
            result, "legacy completed rows resume without phase table/grant"
        )
        legacy_after = self.admin_query(self.target, legacy_query)
        if legacy_after != legacy_before:
            raise HarnessError(
                f"full legacy records changed: before={legacy_before!r} after={legacy_after!r}"
            )
        for endpoint in (self.source, self.target):
            for table in tables:
                actual = self.admin_query(
                    endpoint, f"SELECT * FROM `{table}` ORDER BY id;"
                )
                if actual != data_before[(endpoint.container, table)]:
                    raise HarnessError(
                        f"legacy resume changed {endpoint.container}.{table}: {actual!r}"
                    )
            user = SOURCE_USER if endpoint == self.source else SYNC_TARGET_USER
            statements = self.admin_query(
                endpoint,
                "SELECT argument FROM mysql.general_log "
                f"WHERE user_host LIKE '{user}%' "
                "AND command_type IN ('Query','Execute','Prepare') ORDER BY event_time;",
            ).splitlines()
            if not statements:
                raise HarnessError(
                    f"missing database query evidence: {endpoint.container}"
                )
            for statement in statements:
                normalized = statement.replace("`", "").lower()
                if "sync_runs_phases" in normalized or any(
                    re.search(
                        rf"\b(?:from|join|update|into)\s+(?:{APP_SCHEMA}\.)?{table}\b",
                        normalized,
                    )
                    for table in tables
                ):
                    raise HarnessError(
                        f"legacy resume accessed phase table or application rows: {endpoint.container}: {statement}"
                    )
        phases = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA='cdc' AND TABLE_NAME='sync_runs_phases';",
        ).strip()
        final = self.admin_query(
            self.target,
            "SELECT table_name,status FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} AND stage='final_constraints' ORDER BY table_name;",
        ).strip()
        if phases != "0" or final != "legacy_child\tcomplete\nlegacy_parent\tcomplete":
            raise HarnessError(
                f"legacy final constraints/phase absence failed: final={final!r} phases={phases}"
            )
        print(
            "sync_legacy_complete_resume_ok legacy_full_records_unchanged=4 "
            "source_and_target_data_unchanged=true row_access=0 phase_access=0 "
            "phase_table_absent=true final_constraints_complete=2"
        )

    def run_sync_resume(self) -> None:
        assert self.source and self.target
        run_id = "sync-resume"
        complete_table = "sync_resume_a_complete"
        running_table = "sync_resume_z_running"
        tables = [complete_table, running_table]
        for table in tables:
            self.setup_sync_accounts(table)

        complete_values = ",".join(
            f"({index}, 'complete-{index}', 'source-{index}')" for index in range(1, 31)
        )
        running_values = ",".join(
            f"({index}, 'running-{index}', 'source-{index}')"
            for index in range(1, 4001)
        )
        self.admin_sql(self.source, f"INSERT INTO {complete_table} VALUES {complete_values};")
        self.admin_sql(self.source, f"INSERT INTO {running_table} VALUES {running_values};")

        process, log_path = self.start_sync(
            tables=tables,
            run_id=run_id,
            chunk_size=10,
            parallelism=1,
        )
        deadline = time.monotonic() + 90
        boundary = ""
        try:
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise HarnessError(
                        f"sync resume exited before interruption: {log_path.read_text()}"
                    )
                boundary = self.admin_query(
                    self.target,
                    "SELECT table_name,status,chunks FROM cdc.sync_runs "
                    f"WHERE run_id={sql_literal(run_id)} AND stage='rows' "
                    "ORDER BY table_name;",
                ).strip()
                rows = [line.split("\t") for line in boundary.splitlines()]
                progress = {
                    fields[0]: (fields[1], int(fields[2]))
                    for fields in rows
                    if len(fields) == 3
                }
                complete = progress.get(complete_table)
                running = progress.get(running_table)
                if (
                    complete is not None
                    and complete[0] == "complete"
                    and running is not None
                    and running[0] == "running"
                    and running[1] >= 150
                ):
                    break
                time.sleep(0.05)
            else:
                raise HarnessError(
                    f"sync resume did not reach complete/running boundary: {boundary!r}"
                )
        finally:
            self.stop_sync_process(process)

        legacy_specs = {
            complete_table: '{"legacy":"completed-table"}',
            running_table: '{"legacy":"running-table"}',
        }
        for table, value in legacy_specs.items():
            self.admin_sql(
                self.target,
                "UPDATE cdc.sync_runs "
                f"SET run_spec_json={sql_literal(value)} "
                f"WHERE run_id={sql_literal(run_id)} AND stage='rows' "
                f"AND table_name={sql_literal(table)};",
            )

        complete_before = self.sync_row_progress_evidence(run_id, complete_table)
        running_before = self.sync_row_progress_evidence(run_id, running_table)
        if complete_before["status"] != "complete" or running_before["status"] != "running":
            raise HarnessError(
                "interrupted sync did not retain exact complete/running boundary: "
                f"complete={complete_before!r} running={running_before!r}"
            )
        if int(running_before["chunks"]) < 150:
            raise HarnessError(f"running progress regressed before resume: {running_before!r}")

        source_before = {table: self.sync_table_state(self.source, table) for table in tables}
        target_before = {table: self.sync_table_state(self.target, table) for table in tables}
        if source_before[complete_table] != target_before[complete_table]:
            raise HarnessError(
                "completed table was not converged before resume: "
                f"source={source_before[complete_table]} target={target_before[complete_table]}"
            )
        running_source_count = source_before[running_table][0]
        running_target_count, running_target_distinct, _ = target_before[running_table]
        if not 0 < running_target_count < running_source_count:
            raise HarnessError(
                "running table target was not partially converged before resume: "
                f"source={source_before[running_table]} target={target_before[running_table]}"
            )
        if running_target_count != running_target_distinct:
            raise HarnessError(f"running table had duplicate target PKs: {target_before[running_table]}")
        legacy_before = {
            table: self.sync_legacy_run_spec(run_id, table) for table in tables
        }
        if legacy_before != legacy_specs:
            raise HarnessError(f"legacy run specifications were not installed: {legacy_before!r}")

        resumed = self.run_sync(
            tables=tables,
            run_id=run_id,
            chunk_size=37,
            parallelism=16,
            target_host="localhost",
            timeout=240,
        )
        require_success(resumed, "same-run changed-address sync resume")
        resume_output = "\n".join((resumed.stdout, resumed.stderr)).lower()
        forbidden_output = [
            marker
            for marker in ("run specification", "run_spec", "authoriz")
            if marker in resume_output
        ]
        if forbidden_output:
            raise HarnessError(
                f"sync resume entered obsolete run-spec path: {forbidden_output!r}"
            )

        complete_after = self.sync_row_progress_evidence(run_id, complete_table)
        running_after = self.sync_row_progress_evidence(run_id, running_table)
        if complete_after != complete_before:
            raise HarnessError(
                "completed table row progress changed during resume: "
                f"before={complete_before!r} after={complete_after!r}"
            )
        if running_after["status"] != "complete":
            raise HarnessError(f"running table did not complete: {running_after!r}")
        for counter in (
            "chunks",
            "rows_scanned",
            "inserts_applied",
            "updates_applied",
            "deletes_applied",
        ):
            if int(running_after[counter]) < int(running_before[counter]):
                raise HarnessError(
                    f"running table {counter} regressed: "
                    f"before={running_before[counter]} after={running_after[counter]}"
                )
        if int(running_after["chunks"]) <= int(running_before["chunks"]):
            raise HarnessError(
                "running table did not advance from persisted chunks: "
                f"before={running_before!r} after={running_after!r}"
            )
        before_primary_key = int(json.loads(running_before["last_primary_key_json"])[0])
        after_primary_key = int(json.loads(running_after["last_primary_key_json"])[0])
        if after_primary_key <= before_primary_key:
            raise HarnessError(
                "running table did not advance from persisted cursor: "
                f"before={before_primary_key} after={after_primary_key}"
            )
        if running_after["created_at"] != running_before["created_at"]:
            raise HarnessError("running table progress was recreated instead of resumed")

        legacy_after = {
            table: self.sync_legacy_run_spec(run_id, table) for table in tables
        }
        if legacy_after != legacy_before:
            raise HarnessError(
                f"resume rewrote ignored legacy run specifications: {legacy_after!r}"
            )

        final_states = {
            table: (
                self.sync_table_state(self.source, table),
                self.sync_table_state(self.target, table),
            )
            for table in tables
        }
        for table, (source_state, target_state) in final_states.items():
            if source_state != target_state:
                raise HarnessError(
                    f"same-run resume left table `{table}` divergent: "
                    f"source={source_state} target={target_state}"
                )
            if source_state[0] != source_state[1]:
                raise HarnessError(f"same-run resume left duplicate PKs in `{table}`: {source_state}")

        run_identity = self.admin_query(
            self.target,
            "SELECT COUNT(DISTINCT run_id),MIN(run_id),MAX(run_id),COUNT(*) "
            "FROM cdc.sync_runs WHERE run_id LIKE 'sync-resume%';",
        ).strip()
        if run_identity != "1\tsync-resume\tsync-resume\t6":
            raise HarnessError(f"sync resume created unexpected run identity: {run_identity!r}")
        stages = self.admin_query(
            self.target,
            "SELECT stage,status,COUNT(*) FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} GROUP BY stage,status "
            "ORDER BY FIELD(stage,'prerequisite_schema','rows','final_constraints'),status;",
        ).splitlines()
        expected_stages = [
            "prerequisite_schema\tcomplete\t2",
            "rows\tcomplete\t2",
            "final_constraints\tcomplete\t2",
        ]
        if stages != expected_stages:
            raise HarnessError(f"sync resume final stage progress mismatch: {stages!r}")
        nonterminal = self.admin_query(
            self.target,
            "SELECT COUNT(*) FROM cdc.sync_runs "
            f"WHERE run_id={sql_literal(run_id)} AND status IN ('running','error');",
        ).strip()
        if nonterminal != "0":
            raise HarnessError(f"sync resume retained nonterminal progress rows: {nonterminal}")

        print(
            "sync_resume_ok same_run_id=true target_address_changed=true parallelism=16 "
            "completed_table_preserved=true running_table_resumed=true "
            f"completed_chunks={complete_before['chunks']} "
            f"completed_pk={complete_before['last_primary_key_json']} "
            f"running_chunks_before={running_before['chunks']} "
            f"running_rows_before={running_before['rows_scanned']} "
            f"running_pk_before={running_before['last_primary_key_json']} "
            f"running_chunks_after={running_after['chunks']} "
            f"running_rows_after={running_after['rows_scanned']} "
            f"running_pk_after={running_after['last_primary_key_json']} "
            "legacy_specs_unchanged=true final_stage_rows=6"
        )

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
        elif scenario == "sync-enum-append":
            self.run_sync_enum_append()
        elif scenario == "sync-enum-incompatible":
            self.run_sync_enum_incompatible()
        elif scenario == "sync-composite-enum-primary-key":
            self.run_sync_composite_enum_primary_key()
        elif scenario == "sync-parent-only-constraints-preserved":
            self.run_sync_constraints_preserved(parent_only=True)
        elif scenario == "sync-constraints-preserved":
            self.run_sync_constraints_preserved()
        elif scenario == "sync-fk-parent-insert":
            self.run_sync_fk_parent_convergence()
        elif scenario == "sync-fk-parent-update":
            self.run_sync_fk_parent_convergence(update_existing_child=True)
        elif scenario == "sync-fk-restrict-key-transition-cursor-resume":
            self.run_sync_fk_restrict_key_transition(variant="cursor-resume")
        elif scenario == "sync-fk-restrict-key-transition":
            self.run_sync_fk_restrict_key_transition()
        elif scenario == "sync-fk-restrict-key-transition-reparent":
            self.run_sync_fk_restrict_key_transition("reparent")
        elif scenario == "sync-fk-restrict-key-transition-rollback-resume":
            self.run_sync_fk_restrict_key_transition("rollback-resume")
        elif scenario == "sync-fk-restrict-key-transition-new-child":
            self.run_sync_fk_restrict_key_transition("new-child")
        elif scenario == "sync-fk-parent-stale-unique-owner":
            self.run_sync_fk_parent_stale_unique_owner()
        elif scenario == "sync-fk-source-absent-unique-owner":
            self.run_sync_fk_source_absent_unique_owner()
        elif scenario == "sync-update-stale-unique-owner-rollback-resume":
            self.run_sync_update_stale_unique_owner_rollback_resume()
        elif scenario == "sync-unique-owner-rollback-resume":
            self.run_sync_unique_owner_rollback_resume()
        elif scenario == "sync-wide-update":
            self.run_sync_wide_update()
        elif scenario == "sync-bit-values":
            self.run_sync_bit_values()
        elif scenario == "sync-resume":
            self.run_sync_resume()
        elif scenario == "sync-legacy-complete-resume":
            self.run_sync_legacy_complete_resume()
        elif scenario == "repair-fk-orphans-parents":
            self.run_repair_fk_orphans_parents()
        elif scenario == "repair-guest-range":
            self.run_repair_guest_range()
        elif scenario == "sync-schema-parallel-resume":
            self.run_sync_schema_parallel_resume()
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
