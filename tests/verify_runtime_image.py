#!/usr/bin/env python3
"""Behaviorally verify the built CDC runtime container image."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from dataclasses import dataclass


EXPECTED_ENTRYPOINT = ["mariadb-mysql-cdc"]
EXPECTED_USER = "65532:65532"
REQUIRED_PACKAGES = (
    "ca-certificates",
    "libc6",
    "libgcc-s1",
    "libmariadb3",
    "libssl3t64",
    "zlib1g",
)


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def run_command(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(arguments, capture_output=True, check=False, text=True)


def inspect_image(image: str) -> dict[str, object]:
    result = run_command(["docker", "image", "inspect", image])
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"unable to inspect {image}")
    images = json.loads(result.stdout)
    if len(images) != 1:
        raise RuntimeError(f"expected one inspected image, got {len(images)}")
    return images[0]


def run_in_image(
    image: str,
    *arguments: str,
    entrypoint: str | None = None,
    docker_arguments: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    command = ["docker", "run", "--rm", *docker_arguments]
    if entrypoint is not None:
        command.extend(["--entrypoint", entrypoint])
    command.append(image)
    command.extend(arguments)
    return run_command(command)


def metadata_checks(metadata: dict[str, object]) -> list[CheckResult]:
    config = metadata.get("Config")
    if not isinstance(config, dict):
        return [CheckResult("image config", False, "Config metadata is absent")]

    entrypoint = config.get("Entrypoint")
    user = config.get("User")
    size = metadata.get("Size")
    return [
        CheckResult(
            "direct entrypoint",
            entrypoint == EXPECTED_ENTRYPOINT,
            f"expected {EXPECTED_ENTRYPOINT!r}, got {entrypoint!r}",
        ),
        CheckResult(
            "fixed numeric user",
            user == EXPECTED_USER,
            f"expected {EXPECTED_USER!r}, got {user!r}",
        ),
        CheckResult(
            "image size metadata",
            isinstance(size, int) and size > 0,
            f"reported size is {size!r}",
        ),
    ]


def command_check(
    name: str,
    result: subprocess.CompletedProcess[str],
    failure: str,
) -> CheckResult:
    passed = result.returncode == 0
    if passed:
        return CheckResult(name, True, "command succeeded")
    output = (result.stdout + result.stderr).strip()
    return CheckResult(name, False, output or failure)


def mounted_ca_file_check(image: str) -> CheckResult:
    with tempfile.TemporaryDirectory() as temporary_directory:
        ca_file = Path(temporary_directory) / "do-ca.pem"
        ca_file.write_text("runtime CA mount fixture\n")
        ca_file.chmod(0o644)
        result = run_in_image(
            image,
            "-eu",
            "-c",
            f'test "$(id -u):$(id -g)" = "{EXPECTED_USER}"; '
            "test -s /tmp/do-ca.pem; test -r /tmp/do-ca.pem; test ! -w /tmp/do-ca.pem",
            entrypoint="/bin/sh",
            docker_arguments=(
                "--mount",
                f"type=bind,source={ca_file},target=/tmp/do-ca.pem,readonly",
            ),
        )
    return command_check(
        "read-only mounted CA file",
        result,
        f"a read-only CA file was not readable without write access under {EXPECTED_USER}",
    )


def runtime_checks(image: str) -> list[CheckResult]:
    package_script = "\n".join(
        f"dpkg-query -W -f='${{Status}}' {package} | grep -qx 'install ok installed'"
        for package in REQUIRED_PACKAGES
    )
    return [
        command_check(
            "Ubuntu 24.04 runtime",
            run_in_image(
                image,
                "-c",
                ". /etc/os-release; test \"$ID:$VERSION_ID\" = \"ubuntu:24.04\"",
                entrypoint="/bin/sh",
            ),
            "runtime is not Ubuntu 24.04",
        ),
        command_check(
            "runtime numeric identity",
            run_in_image(
                image,
                "-c",
                f'test "$(id -u):$(id -g)" = "{EXPECTED_USER}"',
                entrypoint="/bin/sh",
            ),
            f"container did not run as {EXPECTED_USER}",
        ),
        command_check(
            "gosu absent",
            run_in_image(
                image,
                "-c",
                "test ! -e /usr/local/bin/gosu && ! command -v gosu >/dev/null 2>&1",
                entrypoint="/bin/sh",
            ),
            "gosu exists in the runtime image",
        ),
        command_check(
            "CA certificate bundle",
            run_in_image(
                image,
                "-c",
                "test -s /etc/ssl/certs/ca-certificates.crt",
                entrypoint="/bin/sh",
            ),
            "CA certificate bundle is absent or empty",
        ),
        mounted_ca_file_check(image),
        command_check(
            "required runtime packages",
            run_in_image(image, "-eu", "-c", package_script, entrypoint="/bin/sh"),
            f"one or more required packages are missing: {', '.join(REQUIRED_PACKAGES)}",
        ),
        command_check(
            "linked runtime libraries",
            run_in_image(
                image,
                "-eu",
                "-c",
                "ldd /usr/local/bin/mariadb-mysql-cdc > /tmp/ldd.out; "
                "! grep -q 'not found' /tmp/ldd.out",
                entrypoint="/bin/sh",
            ),
            "one or more linked runtime libraries are unresolved",
        ),
        command_check(
            "direct entrypoint execution",
            run_in_image(image, "--help"),
            "direct entrypoint did not execute successfully",
        ),
    ]


def verify_image(image: str) -> list[CheckResult]:
    metadata = inspect_image(image)
    return metadata_checks(metadata) + runtime_checks(image)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image", help="local or pullable container image reference")
    arguments = parser.parse_args()

    try:
        results = verify_image(arguments.image)
    except (RuntimeError, json.JSONDecodeError, OSError) as error:
        print(f"[FAIL] verifier setup — {error}", file=sys.stderr)
        return 1

    failures = [result for result in results if not result.passed]
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.name} — {result.detail}")

    if failures:
        print(f"runtime image verification failed: {len(failures)} check(s)", file=sys.stderr)
        return 1

    print(f"runtime image verification passed: {len(results)} check(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
