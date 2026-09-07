#!/usr/bin/env python3
"""Bounded MariaDB -> MySQL datatype round-trip integration audit.

Creates disposable source and target containers through the existing CDC
integration Harness, syncs finite fixtures with the supplied binary, and emits
a result for every source datatype observed in the recorded schema metadata.
The only legacy compatibility adjustment is source-session sql_mode='' while
seeding zero/partial-zero temporal values and ENUM index 0. Target runtime and
sync SQL modes remain unchanged.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
EXISTING_HARNESS = Path(__file__).with_name("cdc-integration-harness.py")
DEFAULT_BINARY = Path("/tmp/claude/cdc-bit-parallel-candidate-c2becb5")
EXPECTED_BINARY_SHA256 = (
    "441b04f17948813aa64258dda35ecbc309fb03ab0883aecc46ffb57e69356286"
)
METADATA_PATH = REPO / "docs/local/datatype-audit-source-columns-20260907.json"
DEFAULT_JSON_REPORT = REPO / "docs/local/datatype-audit-baseline-20260907.json"
DEFAULT_MARKDOWN_REPORT = REPO / "docs/local/datatype-audit-baseline-20260907.md"


def load_existing_harness() -> Any:
    spec = importlib.util.spec_from_file_location(
        "cdc_integration_harness", EXISTING_HARNESS
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load existing Harness from {EXISTING_HARNESS}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


existing = load_existing_harness()
Harness = existing.Harness
HarnessError = existing.HarnessError
sql_literal = existing.sql_literal
require_success = existing.require_success


@dataclass(frozen=True)
class Column:
    name: str
    definition: str
    kind: str


@dataclass(frozen=True)
class Case:
    family: str
    columns: tuple[Column, ...]
    source_initial: tuple[tuple[str, ...], ...]
    source_updates: tuple[tuple[int, tuple[str, ...]], ...]
    source_inserts: tuple[tuple[int, tuple[str, ...]], ...]
    target_initial: tuple[tuple[int, tuple[str, ...]], ...]
    source_legacy_sql_mode: bool = False

    @property
    def table(self) -> str:
        return f"datatype_audit_{self.family}"


@dataclass
class CaseResult:
    family: str
    status: str
    error: str | None
    source_rows: list[list[str]] | None
    target_rows: list[list[str]] | None
    target_unchanged_after_failure: bool | None
    progress: dict[str, str] | None


def columns(*values: tuple[str, str, str]) -> tuple[Column, ...]:
    return tuple(Column(*value) for value in values)


def integer_case(
    family: str,
    signed_definition: str,
    unsigned_definition: str,
    minimum: str,
    maximum: str,
    unsigned_maximum: str,
) -> Case:
    return Case(
        family=family,
        columns=columns(
            ("signed_value", signed_definition, "numeric"),
            ("unsigned_value", unsigned_definition, "numeric"),
        ),
        source_initial=(("0", "0"), ("0", "0")),
        source_updates=((1, (minimum, "0")), (3, (maximum, unsigned_maximum))),
        source_inserts=((4, ("-1", "1")), (5, ("1", "2")), (6, ("NULL", "NULL"))),
        target_initial=((1, ("0", "0")), (2, ("1", "1")), (3, ("0", "0"))),
    )


def text_case(family: str, definition: str) -> Case:
    return Case(
        family=family,
        columns=columns(("value", definition, "hex")),
        source_initial=((text_value("initial"),), (text_value("before-case"),)),
        source_updates=(
            (1, (text_value("unicode ☃ quotes ' \" slash\\ NUL\x00end"),)),
            (3, (text_value("CASE value"),)),
        ),
        source_inserts=(
            (4, (text_value(""),)),
            (5, (text_value("second page"),)),
            (6, ("NULL",)),
        ),
        target_initial=(
            (1, (text_value("target stale"),)),
            (2, (text_value("target only"),)),
            (3, (text_value("wrong"),)),
        ),
    )


def text_value(value: str) -> str:
    return f"CONVERT(0x{value.encode('utf-8').hex()} USING utf8mb4)"


CASES = (
    integer_case(
        "bigint",
        "BIGINT NULL",
        "BIGINT UNSIGNED NULL",
        "-9223372036854775808",
        "9223372036854775807",
        "18446744073709551615",
    ),
    Case(
        family="bit",
        columns=columns(
            ("bit1", "BIT(1) NULL", "bit"),
            ("bit9", "BIT(9) NULL", "bit"),
            ("bit64", "BIT(64) NULL", "bit"),
        ),
        source_initial=(
            ("b'0'", "b'000000000'", "0x0000000000000000"),
            ("b'1'", "b'000000001'", "0x0000000000000001"),
        ),
        source_updates=(
            (1, ("b'1'", "b'100000001'", "0xFFFFFFFFFFFFFFFF")),
            (3, ("b'0'", "b'011111111'", "0x8000000000000000")),
        ),
        source_inserts=(
            (4, ("b'1'", "b'111111111'", "0x0100000000000000")),
            (5, ("b'0'", "b'000000000'", "0x0000000000000000")),
            (6, ("NULL", "NULL", "NULL")),
        ),
        target_initial=(
            (1, ("b'0'", "b'000000000'", "0x0000000000000000")),
            (2, ("b'1'", "b'000000001'", "0x0000000000000001")),
            (3, ("b'1'", "b'000000000'", "0x0000000000000000")),
        ),
    ),
    text_case("char", "CHAR(64) CHARACTER SET utf8mb4 NULL"),
    Case(
        family="date",
        columns=columns(("value", "DATE NULL", "temporal")),
        source_initial=(("'2024-01-01'",), ("'2024-01-02'",)),
        source_updates=((1, ("'9999-12-31'",)), (3, ("'0000-00-00'",))),
        source_inserts=((4, ("'2024-02-29'",)), (5, ("'2010-00-00'",)), (6, ("NULL",))),
        target_initial=(
            (1, ("'2000-01-01'",)),
            (2, ("'2001-01-01'",)),
            (3, ("'2002-01-01'",)),
        ),
        source_legacy_sql_mode=True,
    ),
    Case(
        family="datetime",
        columns=columns(("value", "DATETIME(6) NULL", "temporal")),
        source_initial=(
            ("'2024-01-01 00:00:00.000000'",),
            ("'2024-01-02 00:00:00.000000'",),
        ),
        source_updates=(
            (1, ("'9999-12-31 23:59:59.999999'",)),
            (3, ("'0000-00-00 00:00:00.000000'",)),
        ),
        source_inserts=(
            (4, ("'2024-02-29 12:34:56.123456'",)),
            (5, ("'2010-00-00 00:00:00.000000'",)),
            (6, ("NULL",)),
        ),
        target_initial=(
            (1, ("'2000-01-01 00:00:00'",)),
            (2, ("'2001-01-01 00:00:00'",)),
            (3, ("'2002-01-01 00:00:00'",)),
        ),
        source_legacy_sql_mode=True,
    ),
    Case(
        family="decimal",
        columns=columns(("value", "DECIMAL(30,10) NULL", "numeric")),
        source_initial=(("0.0000000000",), ("1.0000000000",)),
        source_updates=(
            (1, ("-99999999999999999999.1234567890",)),
            (3, ("99999999999999999999.1234567890",)),
        ),
        source_inserts=(
            (4, ("-0.0000000001",)),
            (5, ("12345678901234567890.0000000001",)),
            (6, ("NULL",)),
        ),
        target_initial=(
            (1, ("1.0000000000",)),
            (2, ("2.0000000000",)),
            (3, ("3.0000000000",)),
        ),
    ),
    Case(
        family="double",
        columns=columns(("value", "DOUBLE NULL", "numeric")),
        source_initial=(("0",), ("1",)),
        source_updates=((1, ("-9007199254740992",)), (3, ("9007199254740992",))),
        source_inserts=(
            (4, ("1.5",)),
            (5, ("0.0000000000000002220446049250313",)),
            (6, ("NULL",)),
        ),
        target_initial=((1, ("1",)), (2, ("2",)), (3, ("3",))),
    ),
    Case(
        family="enum",
        columns=columns(("value", "ENUM('','ordinary','1','2') NULL", "enum")),
        source_initial=(("'ordinary'",), ("''",)),
        source_updates=((1, ("'2'",)), (3, ("0",))),
        source_inserts=((4, ("'1'",)), (5, ("''",)), (6, ("NULL",))),
        target_initial=((1, ("'1'",)), (2, ("'ordinary'",)), (3, ("'ordinary'",))),
        source_legacy_sql_mode=True,
    ),
    Case(
        family="float",
        columns=columns(("value", "FLOAT NULL", "numeric")),
        source_initial=(("0",), ("1",)),
        source_updates=((1, ("-16777216",)), (3, ("16777216",))),
        source_inserts=(
            (4, ("1.5",)),
            (5, ("0.00000011920928955078125",)),
            (6, ("NULL",)),
        ),
        target_initial=((1, ("1",)), (2, ("2",)), (3, ("3",))),
    ),
    integer_case(
        "int",
        "INT NULL",
        "INT UNSIGNED NULL",
        "-2147483648",
        "2147483647",
        "4294967295",
    ),
    text_case("longtext", "LONGTEXT CHARACTER SET utf8mb4 NULL"),
    Case(
        family="mediumblob",
        columns=columns(("value", "MEDIUMBLOB NULL", "hex")),
        source_initial=(("X'00'",), ("X'01'",)),
        source_updates=((1, ("X'00FF80FE004142'",)), (3, ("X'FF0080FE'",))),
        source_inserts=(
            (4, ("X''",)),
            (5, ("X'00010203040506070809'",)),
            (6, ("NULL",)),
        ),
        target_initial=((1, ("X'01'",)), (2, ("X'02'",)), (3, ("X'03'",))),
    ),
    integer_case(
        "mediumint",
        "MEDIUMINT NULL",
        "MEDIUMINT UNSIGNED NULL",
        "-8388608",
        "8388607",
        "16777215",
    ),
    text_case("mediumtext", "MEDIUMTEXT CHARACTER SET utf8mb4 NULL"),
    integer_case(
        "smallint",
        "SMALLINT NULL",
        "SMALLINT UNSIGNED NULL",
        "-32768",
        "32767",
        "65535",
    ),
    text_case("text", "TEXT CHARACTER SET utf8mb4 NULL"),
    Case(
        family="timestamp",
        columns=columns(("value", "TIMESTAMP(6) NULL", "temporal")),
        source_initial=(
            ("'2024-01-01 00:00:00.000000'",),
            ("'2024-01-02 00:00:00.000000'",),
        ),
        source_updates=(
            (1, ("'2038-01-19 03:14:07.999999'",)),
            (3, ("'0000-00-00 00:00:00.000000'",)),
        ),
        source_inserts=(
            (4, ("'1970-01-01 00:00:01.000000'",)),
            (5, ("'2010-00-00 00:00:00.000000'",)),
            (6, ("NULL",)),
        ),
        target_initial=(
            (1, ("'2000-01-01 00:00:00'",)),
            (2, ("'2001-01-01 00:00:00'",)),
            (3, ("'2002-01-01 00:00:00'",)),
        ),
        source_legacy_sql_mode=True,
    ),
    integer_case(
        "tinyint", "TINYINT NULL", "TINYINT UNSIGNED NULL", "-128", "127", "255"
    ),
    text_case("varchar", "VARCHAR(255) CHARACTER SET utf8mb4 NULL"),
)


def metadata_type_counts(path: Path) -> dict[str, int]:
    document = json.loads(path.read_text())
    rows = document["columns"] if isinstance(document, dict) else document
    counts: dict[str, int] = {}
    for row in rows:
        data_type = row["data_type"]
        counts[data_type] = counts.get(data_type, 0) + 1
    return dict(sorted(counts.items()))


def validate_case_coverage(type_counts: dict[str, int]) -> None:
    observed = set(type_counts)
    covered = {case.family for case in CASES}
    if observed != covered:
        raise HarnessError(
            f"case coverage does not equal observed type families: observed={sorted(observed)} covered={sorted(covered)}"
        )
    if len(CASES) != 19:
        raise HarnessError(f"expected 19 datatype cases, found {len(CASES)}")


def sql_rows(rows: tuple[tuple[int, tuple[str, ...]], ...]) -> str:
    return ",".join(
        "(" + ",".join((str(row_id), *values)) + ")" for row_id, values in rows
    )


def create_schema(case: Case) -> str:
    definitions = [
        "id BIGINT NOT NULL PRIMARY KEY",
        *(f"{column.name} {column.definition}" for column in case.columns),
    ]
    return f"DROP TABLE IF EXISTS {case.table}; CREATE TABLE {case.table} ({','.join(definitions)}) ENGINE=InnoDB;"


def seed_source_sql(case: Case) -> str:
    initial = tuple(zip((1, 3), case.source_initial, strict=True))
    updates = ",".join(
        f"{column.name}=CASE id "
        + " ".join(
            f"WHEN {row_id} THEN {values[index]}"
            for row_id, values in case.source_updates
        )
        + f" ELSE {column.name} END"
        for index, column in enumerate(case.columns)
    )
    inserts = sql_rows(case.source_inserts)
    statements = []
    if case.source_legacy_sql_mode:
        statements.append("SET SESSION sql_mode=''")
    statements.extend(
        (
            f"INSERT INTO {case.table} (id,{','.join(column.name for column in case.columns)}) VALUES {sql_rows(initial)}",
            f"UPDATE {case.table} SET {updates} WHERE id IN (1,3)",
            f"INSERT INTO {case.table} (id,{','.join(column.name for column in case.columns)}) VALUES {inserts}",
        )
    )
    return ";".join(statements) + ";"


def seed_target_sql(case: Case) -> str:
    rows = sql_rows(case.target_initial)
    names = ",".join(column.name for column in case.columns)
    return f"INSERT INTO {case.table} (id,{names}) VALUES {rows};"


def projection(column: Column) -> str:
    value = column.name
    if column.kind == "hex":
        return f"IF({value} IS NULL,'<NULL>',HEX({value}))"
    if column.kind == "bit":
        return f"IF({value} IS NULL,'<NULL>',CONCAT(HEX({value}),':',CAST({value} AS UNSIGNED)))"
    if column.kind == "enum":
        return f"IF({value} IS NULL,'<NULL>',CONCAT(HEX({value}),':',CAST({value} AS UNSIGNED)))"
    if column.kind == "temporal":
        return f"IF({value} IS NULL,'<NULL>',CAST({value} AS CHAR))"
    return f"IF({value} IS NULL,'<NULL>',CAST({value} AS CHAR))"


def fetch_rows(harness: Any, endpoint: Any, case: Case) -> list[list[str]]:
    values = ",".join(("id", *(projection(column) for column in case.columns)))
    rows = harness.admin_query(
        endpoint, f"SELECT {values} FROM {case.table} ORDER BY id;"
    ).splitlines()
    return [row.split("\t") for row in rows]


def normalize_value(value: str, kind: str) -> str:
    if value == "<NULL>" or kind not in {"numeric"}:
        return value
    try:
        decimal = Decimal(value)
    except InvalidOperation as error:
        raise HarnessError(f"numeric result is not parseable: {value!r}") from error
    return format(decimal.normalize(), "f")


def normalize_rows(rows: list[list[str]], case: Case) -> list[list[str]]:
    return [
        [
            row[0],
            *(
                normalize_value(value, column.kind)
                for value, column in zip(row[1:], case.columns, strict=True)
            ),
        ]
        for row in rows
    ]


def progress_evidence(harness: Any, run_id: str, case: Case) -> dict[str, str] | None:
    try:
        return harness.sync_row_progress_evidence(run_id, case.table)
    except HarnessError:
        return None


def audit_case(harness: Any, case: Case) -> CaseResult:
    assert harness.source and harness.target
    run_id = f"datatype-audit-{case.family}"
    harness.admin_sql(harness.source, create_schema(case))
    harness.admin_sql(harness.target, create_schema(case))
    harness.admin_sql(harness.source, seed_source_sql(case))
    harness.admin_sql(harness.target, seed_target_sql(case))
    before = fetch_rows(harness, harness.target, case)
    try:
        result = harness.run_sync(
            tables=[case.table], run_id=run_id, chunk_size=2, timeout=180
        )
        require_success(result, f"datatype audit {case.family}")
        source_rows = fetch_rows(harness, harness.source, case)
        target_rows = fetch_rows(harness, harness.target, case)
        if normalize_rows(source_rows, case) != normalize_rows(target_rows, case):
            raise HarnessError(
                f"stored values differ source={source_rows!r} target={target_rows!r}"
            )
        if len(target_rows) != 5:
            raise HarnessError(
                f"expected five source rows after target-only deletion, got {target_rows!r}"
            )
        progress = progress_evidence(harness, run_id, case)
        if progress is None:
            raise HarnessError("missing durable row progress")
        if (
            progress["status"] != "complete"
            or int(progress["chunks"]) < 3
            or int(progress["rows_scanned"]) != 5
        ):
            raise HarnessError(f"unexpected durable progress: {progress}")
        if (
            int(progress["inserts_applied"]) < 2
            or int(progress["updates_applied"]) < 2
            or int(progress["deletes_applied"]) != 1
        ):
            raise HarnessError(
                f"expected insert/update/delete convergence evidence: {progress}"
            )
        return CaseResult(
            case.family, "pass", None, source_rows, target_rows, None, progress
        )
    except (
        HarnessError,
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ) as error:
        after = fetch_rows(harness, harness.target, case)
        return CaseResult(
            case.family,
            "fail",
            f"{type(error).__name__}: {error}",
            None,
            after,
            after == before,
            progress_evidence(harness, run_id, case),
        )


def binary_revision(binary: Path) -> dict[str, str]:
    if not binary.is_file():
        raise HarnessError(f"audit binary missing: {binary}")
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    if digest != EXPECTED_BINARY_SHA256:
        raise HarnessError(
            f"audit binary hash mismatch: expected={EXPECTED_BINARY_SHA256} actual={digest}"
        )
    return {"path": str(binary), "sha256": digest}


def report_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Datatype round-trip baseline — 2026-09-07",
        "",
        f"- revision: `{report['git_revision']}`",
        f"- binary: `{report['binary']['path']}` ({report['binary']['sha256']})",
        f"- source metadata: `{METADATA_PATH.relative_to(REPO)}`",
        "- source-only legacy SQL mode: `sql_mode=''` for DATE/DATETIME/TIMESTAMP zero or partial-zero fixtures and ENUM index 0; target runtime and sync mode unchanged.",
        "",
        "| family | observed columns | result | durable progress | error |",
        "|---|---:|---|---|---|",
    ]
    for result in report["results"]:
        progress = result["progress"]
        progress_text = (
            "-"
            if progress is None
            else f"{progress['status']}; chunks={progress['chunks']}; rows={progress['rows_scanned']}"
        )
        error = (result["error"] or "").replace("|", "\\|").replace("\n", " ")
        lines.append(
            f"| {result['family']} | {report['source_type_counts'][result['family']]} | {result['status']} | {progress_text} | {error} |"
        )
    lines.extend(
        (
            "",
            f"Overall: **{report['overall_status']}**. Passed {report['passed_cases']}/{report['case_count']} cases.",
        )
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--json-report", type=Path, default=DEFAULT_JSON_REPORT)
    parser.add_argument("--markdown-report", type=Path, default=DEFAULT_MARKDOWN_REPORT)
    parser.add_argument(
        "--keep", action="store_true", help="keep disposable harness containers/files"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    type_counts = metadata_type_counts(METADATA_PATH)
    validate_case_coverage(type_counts)
    revision = existing.run(["git", "rev-parse", "HEAD"], cwd=REPO).stdout.strip()
    report: dict[str, Any] = {
        "audit_date": "2026-09-07",
        "git_revision": revision,
        "binary": binary_revision(args.binary),
        "metadata_path": str(METADATA_PATH.relative_to(REPO)),
        "source_type_counts": type_counts,
        "case_count": len(CASES),
        "legacy_source_sql_mode": "sql_mode='' only while seeding temporal zero/partial-zero values and ENUM index 0; target unchanged",
        "results": [],
    }
    with Harness(REPO, args.binary, args.keep) as harness:
        harness.prepare()
        for case in CASES:
            try:
                result = audit_case(harness, case)
            except (
                HarnessError,
                KeyError,
                OSError,
                RuntimeError,
                TypeError,
                ValueError,
            ) as error:
                result = CaseResult(
                    case.family,
                    "fail",
                    f"setup {type(error).__name__}: {error}",
                    None,
                    None,
                    None,
                    None,
                )
            report["results"].append(asdict(result))
            print(
                f"case_{result.status} family={case.family} error={result.error or '-'}"
            )
    report["passed_cases"] = sum(
        result["status"] == "pass" for result in report["results"]
    )
    report["failed_cases"] = report["case_count"] - report["passed_cases"]
    report["overall_status"] = "pass" if report["failed_cases"] == 0 else "fail"
    args.json_report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    args.markdown_report.write_text(report_markdown(report))
    print(
        f"audit_{report['overall_status']} passed={report['passed_cases']} failed={report['failed_cases']}"
    )
    return 0 if report["overall_status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
