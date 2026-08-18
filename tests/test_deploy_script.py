#!/usr/bin/env python3
"""Behavioral tests for the deployment script's image-release contract."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


REPOSITORY = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPT = REPOSITORY / "deploy.sh"
RUN_TESTS_SCRIPT = REPOSITORY / "run-tests.sh"
IMAGE_REPO = "registry.example/mariadb-mysql-cdc"
IMAGE = f"{IMAGE_REPO}:candidate"
PUBLISHED_DIGEST = "sha256:" + ("a" * 64)
IMMUTABLE_IMAGE = f"{IMAGE}@{PUBLISHED_DIGEST}"
TRIVY_IMAGE = (
    "ghcr.io/aquasecurity/trivy@"
    "sha256:7cced7cae583819fc7806d4cbc0dbbc7cad18b99f7d3e235192e6da8c091045c"
)


def run(
    *arguments: str,
    cwd: Path,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        arguments,
        cwd=cwd,
        env=env,
        capture_output=True,
        check=False,
        text=True,
    )


def initialize_repository(
    path: Path,
    files: dict[str, str],
    executable_paths: tuple[str, ...] = (),
) -> None:
    path.mkdir(parents=True)
    for relative_path, content in files.items():
        destination = path / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content)
    for relative_path in executable_paths:
        (path / relative_path).chmod(0o755)
    for arguments in (
        ("git", "init", "-q"),
        ("git", "config", "user.name", "Runtime Image Test"),
        ("git", "config", "user.email", "runtime-image-test@example.invalid"),
        ("git", "add", "."),
        ("git", "commit", "-qm", "fixture"),
    ):
        result = run(*arguments, cwd=path)
        if result.returncode != 0:
            raise RuntimeError(result.stderr)


def event_logger_source(command: str) -> str:
    return (
        "import json, os, sys\n"
        "with open(os.environ['EVENTS_FILE'], 'a') as output:\n"
        f"    output.write(json.dumps({{'command': '{command}', 'args': sys.argv[1:]}}) + '\\n')\n"
    )


def write_executable(path: Path, content: str) -> None:
    path.write_text("#!/usr/bin/env python3\n" + content)
    path.chmod(0o755)


def initialize_fake_commands(fake_bin: Path) -> None:
    fake_bin.mkdir()
    write_executable(fake_bin / "cargo", event_logger_source("cargo"))
    write_executable(fake_bin / "depot", event_logger_source("depot"))
    write_executable(
        fake_bin / "docker",
        event_logger_source("docker")
        + "arguments = sys.argv[1:]\n"
        + "if arguments[:3] == ['buildx', 'imagetools', 'inspect']:\n"
        + "    print(os.environ['PUBLISHED_DIGEST'])\n"
        + "if arguments and arguments[0] == 'run':\n"
        + "    raise SystemExit(int(os.environ.get('TRIVY_EXIT_CODE', '0')))\n",
    )
    fake_python = fake_bin / "python3"
    fake_python.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "arguments = sys.argv[1:]\n"
        "if arguments == ['-m', 'unittest', 'tests/test_deploy_script.py']:\n"
        "    with open(os.environ['EVENTS_FILE'], 'a') as output:\n"
        "        output.write(json.dumps({'command': 'python3', 'args': arguments}) + '\\n')\n"
        "    raise SystemExit(0)\n"
        + f"os.execv({sys.executable!r}, [{sys.executable!r}, *arguments])\n"
    )
    fake_python.chmod(0o755)
    real_perl = shutil.which("perl")
    if real_perl is None:
        raise RuntimeError("perl is required for deploy-script tests")
    write_executable(
        fake_bin / "perl",
        event_logger_source("perl")
        + f"os.execv({real_perl!r}, [{real_perl!r}, *sys.argv[1:]])\n",
    )


def verifier_fixture() -> str:
    return (
        "import json, os, sys\n"
        "with open(os.environ['EVENTS_FILE'], 'a') as output:\n"
        "    output.write(json.dumps({'command': 'verifier', 'args': sys.argv[1:]}) + '\\n')\n"
        "raise SystemExit(int(os.environ.get('VERIFIER_EXIT_CODE', '0')))\n"
    )


def read_events(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


class DeploymentFixture:
    def __init__(self, root: Path) -> None:
        self.project = root / "cdc"
        self.ops = root / "ops"
        self.fake_bin = root / "bin"
        self.events = root / "events.jsonl"
        initialize_repository(
            self.project,
            {
                "README.md": "fixture\n",
                "deploy.sh": DEPLOY_SCRIPT.read_text(),
                "run-tests.sh": RUN_TESTS_SCRIPT.read_text(),
                "tests/verify_runtime_image.py": verifier_fixture(),
            },
            executable_paths=("deploy.sh", "run-tests.sh"),
        )
        initialize_repository(
            self.ops,
            {
                "infrastructure/ops/mariadb-mysql-cdc-stream.yaml": (
                    "containers:\n"
                    f"- image: {IMAGE_REPO}:old\n"
                )
            },
        )
        initialize_fake_commands(self.fake_bin)

    def environment(
        self,
        *,
        skip_verified_checks: bool = True,
        trivy_exit_code: int = 0,
    ) -> dict[str, str]:
        environment = os.environ.copy()
        environment.update(
            {
                "EVENTS_FILE": str(self.events),
                "IMAGE_REPO": IMAGE_REPO,
                "OPS_REPO": str(self.ops),
                "PATH": f"{self.fake_bin}:{environment['PATH']}",
                "PUBLISHED_DIGEST": PUBLISHED_DIGEST,
                "PUSH_OPS": "0",
                "SKIP_VERIFIED_CHECKS": "1" if skip_verified_checks else "0",
                "TRIVY_EXIT_CODE": str(trivy_exit_code),
            }
        )
        environment.pop("BASE_IMAGE", None)
        return environment

    def deploy(
        self,
        *,
        skip_verified_checks: bool = True,
        trivy_exit_code: int = 0,
    ) -> subprocess.CompletedProcess[str]:
        return run(
            str(self.project / "deploy.sh"),
            "candidate",
            cwd=self.project,
            env=self.environment(
                skip_verified_checks=skip_verified_checks,
                trivy_exit_code=trivy_exit_code,
            ),
        )

    def run_repository_tests(self) -> subprocess.CompletedProcess[str]:
        return run(
            str(self.project / "run-tests.sh"),
            cwd=self.project,
            env=self.environment(),
        )


class DeployScriptTest(unittest.TestCase):
    def test_build_does_not_require_or_forward_base_image(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            fixture = DeploymentFixture(Path(temporary_directory))

            result = fixture.deploy()

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            depot_event = read_events(fixture.events)[0]
            self.assertEqual(depot_event["command"], "depot")
            arguments = depot_event["args"]
            self.assertEqual(arguments[0], "build")
            self.assertNotIn("--build-arg", arguments)
            self.assertFalse(
                any(argument.startswith("BASE_IMAGE=") for argument in arguments)
            )
            self.assertIn(IMAGE, arguments)
            self.assertIn("--push", arguments)

    def test_verifies_pushed_digest_and_scans_before_ops_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            fixture = DeploymentFixture(Path(temporary_directory))

            result = fixture.deploy()

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            events = read_events(fixture.events)
            self.assertEqual(
                [event["command"] for event in events],
                ["depot", "docker", "docker", "verifier", "docker", "perl"],
            )
            self.assertEqual(
                events[1]["args"],
                [
                    "buildx",
                    "imagetools",
                    "inspect",
                    "--format",
                    "{{.Manifest.Digest}}",
                    IMAGE,
                ],
            )
            self.assertEqual(events[2]["args"], ["pull", IMMUTABLE_IMAGE])
            self.assertEqual(events[3]["args"], [IMMUTABLE_IMAGE])
            self.assertEqual(
                events[4]["args"],
                [
                    "run",
                    "--rm",
                    "--volume",
                    "/var/run/docker.sock:/var/run/docker.sock",
                    TRIVY_IMAGE,
                    "image",
                    "--scanners",
                    "vuln",
                    "--severity",
                    "HIGH,CRITICAL",
                    "--ignore-unfixed",
                    "--skip-version-check",
                    "--exit-code",
                    "1",
                    IMMUTABLE_IMAGE,
                ],
            )
            manifest = (
                fixture.ops
                / "infrastructure/ops/mariadb-mysql-cdc-stream.yaml"
            ).read_text()
            self.assertIn(f"image: {IMMUTABLE_IMAGE}", manifest)
            commit_subject = run(
                "git",
                "log",
                "-1",
                "--pretty=%s",
                cwd=fixture.ops,
            )
            self.assertEqual(commit_subject.stdout.strip(), "Deploy CDC image candidate")

    def test_deploy_runs_repository_tests_between_fmt_and_clippy(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            fixture = DeploymentFixture(Path(temporary_directory))

            result = fixture.deploy(skip_verified_checks=False)

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            events = read_events(fixture.events)
            self.assertEqual(
                events[:4],
                [
                    {"command": "cargo", "args": ["fmt", "--check"]},
                    {"command": "cargo", "args": ["test"]},
                    {
                        "command": "python3",
                        "args": ["-m", "unittest", "tests/test_deploy_script.py"],
                    },
                    {
                        "command": "cargo",
                        "args": [
                            "clippy",
                            "--all-targets",
                            "--all-features",
                            "--",
                            "-D",
                            "warnings",
                        ],
                    },
                ],
            )

    def test_repository_test_path_includes_deploy_contract_tests(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            fixture = DeploymentFixture(Path(temporary_directory))

            result = fixture.run_repository_tests()

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(
                read_events(fixture.events),
                [
                    {"command": "cargo", "args": ["test"]},
                    {
                        "command": "python3",
                        "args": ["-m", "unittest", "tests/test_deploy_script.py"],
                    },
                ],
            )

    def test_failed_trivy_gate_leaves_ops_manifest_and_commit_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            fixture = DeploymentFixture(Path(temporary_directory))
            manifest_path = (
                fixture.ops
                / "infrastructure/ops/mariadb-mysql-cdc-stream.yaml"
            )
            initial_manifest = manifest_path.read_text()
            initial_head = run("git", "rev-parse", "HEAD", cwd=fixture.ops).stdout.strip()

            result = fixture.deploy(trivy_exit_code=1)

            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(manifest_path.read_text(), initial_manifest)
            self.assertEqual(
                run("git", "rev-parse", "HEAD", cwd=fixture.ops).stdout.strip(),
                initial_head,
            )
            self.assertEqual(run("git", "status", "--short", cwd=fixture.ops).stdout, "")
            self.assertEqual(
                [event["command"] for event in read_events(fixture.events)],
                ["depot", "docker", "docker", "verifier", "docker"],
            )


if __name__ == "__main__":
    unittest.main()
