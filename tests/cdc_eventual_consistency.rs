use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn harness_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/cdc-integration-harness.py")
}

fn fixture_paths() -> [PathBuf; 2] {
    [
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/cdc-harness-source-bootstrap.sql"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/cdc-harness-target-bootstrap.sql"),
    ]
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_strict_secondary_btree_harness_smoke() {
    let output = Command::new("python3")
        .arg(harness_script())
        .arg("--scenario")
        .arg("strict-secondary-btree")
        .output()
        .expect("run strict secondary BTREE harness");

    assert!(
        output.status.success(),
        "integration harness failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_create_table_crash_restart_harness_smoke() {
    let output = Command::new("python3")
        .arg(harness_script())
        .arg("--scenario")
        .arg("create-table-crash-restart")
        .output()
        .expect("run CREATE TABLE crash/restart harness");

    assert!(
        output.status.success(),
        "integration harness failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_production_alter_table_harness_smoke() {
    let output = Command::new("python3")
        .arg(harness_script())
        .arg("--scenario")
        .arg("production-alter-table")
        .output()
        .expect("run production ALTER TABLE harness");

    assert!(
        output.status.success(),
        "integration harness failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_catchup_snapshot_tls_harness_smoke() {
    let output = Command::new("python3")
        .arg(harness_script())
        .arg("--scenario")
        .arg("catchup-snapshot-tls")
        .output()
        .expect("run catchup snapshot TLS harness");

    assert!(
        output.status.success(),
        "integration harness failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn source_harness_stream_account_allows_plaintext_transport() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/cdc-harness-source-bootstrap.sql");
    let source_bootstrap = fs::read_to_string(fixture).expect("read source bootstrap fixture");
    assert!(!source_bootstrap.contains("REQUIRE SSL"));
}

#[test]
fn catchup_harness_omits_source_ca_and_preserves_target_ca() {
    let script = harness_script();
    let code = format!(
        r#"""
import importlib.util
import pathlib
import sys

script = pathlib.Path(r'{script}')
spec = importlib.util.spec_from_file_location('cdc_harness', script)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

harness = module.Harness.__new__(module.Harness)
harness.source = module.Endpoint('source', 3307)
harness.target = module.Endpoint('target', 3308)
harness.ca_file = pathlib.Path('/tmp/shared-ca.pem')
args = harness._catchup_args(
    pathlib.Path('/tmp/cdc'),
    pathlib.Path('/tmp/progress.json'),
    target_ca_file=pathlib.Path('/tmp/target-ca.pem'),
)
assert '--source-tls-ca-file' not in args
target_index = args.index('--target-tls-ca-file')
assert args[target_index + 1] == '/tmp/target-ca.pem'
"""#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("check catchup CA policy");
    assert!(
        output.status.success(),
        "catchup CA policy failed:
{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn sync_table_harness_omits_source_ca_and_preserves_target_ca() {
    let script = harness_script();
    let code = format!(
        r#"""
import importlib.util
import pathlib
import sys

script = pathlib.Path(r'{script}')
spec = importlib.util.spec_from_file_location('cdc_harness', script)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

harness = module.Harness.__new__(module.Harness)
harness.source = module.Endpoint('source', 3307)
harness.target = module.Endpoint('target', 3308)
harness.ca_file = pathlib.Path('/tmp/target-ca.pem')
args = harness._sync_table_args(pathlib.Path('/tmp/cdc'))
assert '--source-tls-ca-file' not in args
target_index = args.index('--target-tls-ca-file')
assert args[target_index + 1] == '/tmp/target-ca.pem'
"""#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("check sync-table CA policy");
    assert!(
        output.status.success(),
        "sync-table CA policy failed:
{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn harness_python_source_parses_without_generating_bytecode() {
    let script = harness_script();
    let code = format!(
        "import ast, pathlib; ast.parse(pathlib.Path(r'{}').read_text())",
        script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("parse integration harness Python source");
    assert!(
        output.status.success(),
        "Python syntax failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn harness_default_run_includes_startup_manual_and_recovery_scenarios() {
    let script = harness_script();
    let code = format!(
        r#"
import importlib.util
import pathlib
import sys

script = pathlib.Path(r'{script}')
spec = importlib.util.spec_from_file_location('cdc_harness', script)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

expected = {{
    'missing-checkpoint',
    'missing-trigger',
    'missing-grant',
    'journal-outage',
    'translation-pending-barrier',
    'create-table-crash-restart',
    'prepare-failure',
    'post-ddl-pre-applied',
    'applied-pre-checkpoint',
    'checkpoint-transaction',
    'source-connection-loss',
    'target-connection-loss',
}}
assert expected.issubset(set(module.default_scenarios()))
"#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("check harness default scenarios");
    assert!(
        output.status.success(),
        "default scenario selection failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn harness_translation_pending_requires_nonzero_bounded_termination() {
    let script = harness_script();
    let code = format!(
        r#"
import importlib.util
import pathlib
import sys

script = pathlib.Path(r'{script}')
spec = importlib.util.spec_from_file_location('cdc_harness', script)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

blocked = module.CommandResult(('stream-binlog',), 1, '', 'DDL translator unavailable')
module.require_translation_pending_termination(blocked)

success = module.CommandResult(('stream-binlog',), 0, '', '')
try:
    module.require_translation_pending_termination(success)
except module.HarnessError:
    pass
else:
    raise AssertionError('bounded success without manual block must fail')
"#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("check manual-resolution harness behavior");
    assert!(
        output.status.success(),
        "translation-pending harness check failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn repair_scenarios_are_executable_and_in_repair_scope() {
    let script = fs::read_to_string(harness_script()).expect("read integration harness");
    for scenario in [
        "pre-state-drift",
        "coordinate-reuse",
        "raw-sql-reuse",
        "end-position-reuse",
        "checkpoint-mismatch",
    ] {
        assert!(script.contains(&format!("ScenarioSpec(\"{scenario}\", True)")));
        assert!(script.contains(&format!(
            "self.run_journal_mismatch_scenario(\"{scenario}\")"
        )));
    }
    for scenario in [
        "fk-child-first-delete",
        "fk-parent-first-insert",
        "fk-cycle-block",
        "repair-resume",
        "bounded-delete",
        "conflict-resolution-zero-debt",
    ] {
        assert!(script.contains(&format!("ScenarioSpec(\"{scenario}\", True)")));
    }
}

#[test]
fn repair_scenarios_have_real_cli_orchestration_dispatch() {
    let script = fs::read_to_string(harness_script()).expect("read integration harness");
    assert!(script.contains("def run_repair_scenario"));
    assert!(!script.contains("--source-tls-ca-file"));
    for scenario in [
        "fk-child-first-delete",
        "fk-parent-first-insert",
        "fk-cycle-block",
        "repair-resume",
        "bounded-delete",
        "conflict-resolution-zero-debt",
    ] {
        assert!(
            script.contains(&format!("self.run_repair_scenario(\"{scenario}\")")),
            "missing real repair dispatch for {scenario}"
        );
    }
}

#[test]
fn journal_mismatch_scenarios_assert_fail_closed_state_and_diagnostics() {
    let script = fs::read_to_string(harness_script()).expect("read integration harness");
    for marker in [
        "ddl_replay_journal",
        "stream_checkpoint",
        "canonical_ast",
        "pre_state",
        "expected_post_state",
        "source_server_id",
        "event_end_position",
        "raw_sql",
        "mutated target",
        "advanced checkpoint",
        "no_overtake",
        "identity mismatch",
        "pre-state mismatch",
        "checkpoint predecessor mismatch",
    ] {
        assert!(
            script
                .to_ascii_lowercase()
                .contains(&marker.to_ascii_lowercase()),
            "missing mismatch assertion marker: {marker}"
        );
    }
}

#[test]
fn harness_scenario_listing_has_behavior_or_explicit_prerequisite() {
    let output = Command::new("python3")
        .arg(harness_script())
        .arg("--list")
        .output()
        .expect("run integration harness scenario listing");
    assert!(
        output.status.success(),
        "scenario listing failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let listed = String::from_utf8_lossy(&output.stdout);
    assert!(!listed.lines().any(|line| !line.contains('\t')));
    assert!(
        listed
            .lines()
            .any(|line| line == "strict-secondary-btree\texecutable")
    );
    for scenario in [
        "prepare-failure",
        "post-ddl-pre-applied",
        "applied-pre-checkpoint",
        "checkpoint-transaction",
        "source-connection-loss",
        "target-connection-loss",
        "fk-child-first-delete",
        "fk-parent-first-insert",
        "fk-cycle-block",
        "repair-resume",
        "bounded-delete",
        "conflict-resolution-zero-debt",
    ] {
        assert!(
            listed
                .lines()
                .any(|line| line == format!("{scenario}\texecutable"))
        );
    }
}

#[test]
fn generated_tls_material_is_container_readable_without_disabling_verification() {
    let script = harness_script();
    let code = format!(
        r#"
import importlib.util
import pathlib
import stat
import sys
import tempfile

script = pathlib.Path(r'{script}')
spec = importlib.util.spec_from_file_location('cdc_harness', script)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

with tempfile.TemporaryDirectory() as temp:
    tempdir = pathlib.Path(temp)
    files = [tempdir / name for name in ('ca.pem', 'server-cert.pem', 'server-key.pem')]
    for path in files:
        path.write_text('test')
    module.make_tls_material_container_readable(tempdir, files)
    assert stat.S_IMODE(tempdir.stat().st_mode) & 0o755 == 0o755
    for path in files:
        assert stat.S_IMODE(path.stat().st_mode) & 0o644 == 0o644

source = script.read_text()
assert '--ssl-verify-server-cert' in source
assert '--ssl-verify-server-cert=0' not in source
"#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("check generated TLS material permissions");
    assert!(
        output.status.success(),
        "TLS permission check failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn readiness_diagnostics_include_container_logs() {
    let script = harness_script();
    let code = format!(
        r#"
import importlib.util
import os
import pathlib
import sys
import tempfile

script = pathlib.Path(r'{script}')
spec = importlib.util.spec_from_file_location('cdc_harness', script)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

with tempfile.TemporaryDirectory() as temp:
    bin_dir = pathlib.Path(temp) / 'bin'
    bin_dir.mkdir()
    docker = bin_dir / 'docker'
    docker.write_text(
        '#!/usr/bin/python3\n'
        'import sys\n'
        "print('MariaDB init failed')\n"
        "print('SSL_CTX_set_default_verify_paths failed', file=sys.stderr)\n"
    )
    docker.chmod(0o755)
    old_path = os.environ['PATH']
    os.environ['PATH'] = str(bin_dir) + os.pathsep + old_path
    try:
        logs = module.container_logs('mariadb')
    finally:
        os.environ['PATH'] = old_path

assert 'MariaDB init failed' in logs
assert 'SSL_CTX_set_default_verify_paths failed' in logs
"#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("check readiness diagnostics");
    assert!(
        output.status.success(),
        "readiness diagnostics check failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn harness_accepts_mariadb_binlog_monitor_alias_and_implicit_usage() {
    let script = harness_script();
    let code = format!(
        r#"
import importlib.util
import pathlib
import sys

script = pathlib.Path(r'{script}')
spec = importlib.util.spec_from_file_location('cdc_harness', script)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

actual = module.normalize_grants('''GRANT BINLOG MONITOR, REPLICATION SLAVE ON *.* TO `cdc_reader`@`%`
GRANT SELECT, SHOW VIEW ON globalcomix.* TO `cdc_reader`@`%`''')
module.assert_exact_grants(
    actual,
    {{
        (frozenset({{'USAGE'}}), '*.*'),
        (frozenset({{'REPLICATION SLAVE', 'REPLICATION CLIENT'}}), '*.*'),
        (frozenset({{'SELECT', 'SHOW VIEW'}}), 'globalcomix.*'),
    }},
    'cdc_reader',
)
"#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("check MariaDB privilege normalization");
    assert!(
        output.status.success(),
        "MariaDB privilege normalization failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn harness_rejects_unexpected_global_or_admin_grants() {
    let script = harness_script();
    let code = format!(
        r#"
import importlib.util
import pathlib
import sys

script = pathlib.Path(r'{script}')
spec = importlib.util.spec_from_file_location('cdc_harness', script)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

expected = {{(frozenset({{'SELECT'}}), 'globalcomix.*')}}
for grant in (
    "GRANT SUPER ON *.* TO `cdc_reader`@`%`",
    "GRANT ALL PRIVILEGES ON *.* TO `cdc_reader`@`%`",
):
    try:
        module.assert_exact_grants(module.normalize_grants(
            grant + "\\nGRANT SELECT ON globalcomix.* TO `cdc_reader`@`%`"
        ), expected, 'cdc_reader')
    except module.HarnessError:
        pass
    else:
        raise AssertionError(f'unexpected grant accepted: {{grant}}')
"#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("check broad grant rejection");
    assert!(
        output.status.success(),
        "broad grant rejection failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn harness_has_no_unsafe_runtime_sql_or_tls_flags() {
    let script = fs::read_to_string(harness_script()).expect("read harness script");
    let runtime = section(
        &script,
        "    def _stream_args",
        "    def setup_accounts_table",
    );
    for forbidden in [
        "GRANT ALL",
        "--force",
        "--ssl-verify-server-cert=0",
        "--target-user\",\n            \"root",
    ] {
        assert!(
            !script.contains(forbidden),
            "forbidden harness text: {forbidden}"
        );
    }
    assert!(
        !runtime.contains("root"),
        "CDC runtime path must not use root"
    );
    assert!(runtime.contains("TARGET_USER"));
    assert!(runtime.contains("SOURCE_USER"));
    assert!(!runtime.contains("--source-tls-ca-file"));
    assert!(runtime.contains("--target-tls-ca-file"));
    assert!(!runtime.contains("/etc/mariadb-mysql-cdc/do-ca.pem"));
}

#[test]
fn conflict_startup_rejection_scenarios_fail_before_source_mutation() {
    let script = fs::read_to_string(harness_script()).expect("read integration harness");
    for scenario in [
        "missing-conflict-trigger",
        "missing-conflict-table",
        "wrong-conflict-schema",
        "missing-conflict-grant",
        "broad-conflict-grant",
    ] {
        assert!(script.contains(&format!("ScenarioSpec(\"{scenario}\", True)")));
    }
    assert!(script.contains("DROP TRIGGER cdc.row_conflicts_update_guard"));
    assert!(script.contains("DROP TABLE cdc.row_conflicts"));
    assert!(script.contains("ALTER TABLE cdc.row_conflicts MODIFY status VARCHAR(32)"));
    assert!(script.contains("REVOKE UPDATE ON cdc.row_conflicts"));
    assert!(script.contains("GRANT DELETE ON cdc.row_conflicts"));
    assert!(script.contains("SELECT COUNT(*) FROM globalcomix.accounts"));
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn conflict_runtime_uses_definer_inventory_procedure() {
    let output = Command::new("python3")
        .arg(harness_script())
        .arg("--scenario")
        .arg("missing-conflict-trigger")
        .arg("--binary")
        .arg(env!("CARGO_BIN_EXE_mariadb-mysql-cdc"))
        .output()
        .expect("run missing conflict trigger harness");

    assert!(
        output.status.success(),
        "conflict runtime harness failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("missing-conflict-trigger_rejected boundary=trigger"),
        "conflict runtime harness did not report the startup rejection boundary"
    );
}

#[test]
fn bootstrap_fixtures_provision_exact_conflict_inventory_procedure() {
    let target = fs::read_to_string(&fixture_paths()[1]).expect("read target bootstrap fixture");
    assert!(
        target.contains(
            "CREATE DEFINER=CURRENT_USER PROCEDURE cdc.row_conflicts_trigger_inventory()"
        )
    );
    assert!(target.contains("SQL SECURITY DEFINER"));
    assert!(target.contains("READS SQL DATA"));
    for field in [
        "trigger_name",
        "event_object_schema",
        "event_object_table",
        "event_manipulation",
        "action_timing",
        "action_statement",
        "action_order",
    ] {
        assert!(target.contains(field), "target procedure missing {field}");
    }
    assert!(target.contains("GRANT EXECUTE ON PROCEDURE cdc.row_conflicts_trigger_inventory"));
}

#[test]
fn bootstrap_fixtures_use_exact_restricted_accounts() {
    for path in fixture_paths() {
        assert!(Path::new(&path).is_file(), "missing fixture {path:?}");
        let sql = fs::read_to_string(&path).expect("read bootstrap fixture");
        assert!(!sql.contains("GRANT ALL"));
        assert!(!sql.contains("ALL PRIVILEGES"));
        assert!(!sql.contains("WITH GRANT OPTION"));
    }
    let source = fs::read_to_string(&fixture_paths()[0]).expect("read source fixture");
    assert!(source.contains("cdc_reader"));
    assert!(source.contains("REPLICATION SLAVE, REPLICATION CLIENT ON *.*"));
    let target = fs::read_to_string(&fixture_paths()[1]).expect("read target fixture");
    assert!(target.contains("cdc_stream"));
    assert!(target.contains("ON globalcomix.* TO 'cdc_stream'@'%'"));
    assert!(target.contains("ON cdc.stream_checkpoint TO 'cdc_stream'@'%'"));
    assert!(target.contains("ON cdc.row_conflicts TO 'cdc_stream'@'%'"));
    assert!(target.contains(
        "GRANT EXECUTE ON PROCEDURE cdc.row_conflicts_trigger_inventory TO 'cdc_stream'@'%'"
    ));
    assert!(target.contains("ON cdc.ddl_replay_journal TO 'cdc_stream'@'%'"));
    assert!(!target.contains("source_primary_key_json("));
    for field in [
        "conflict_identity CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL",
        "source_identity VARCHAR(255) NOT NULL",
        "source_server_id BIGINT UNSIGNED NOT NULL",
        "source_primary_key_json TEXT NOT NULL",
        "attempt_count BIGINT UNSIGNED NOT NULL DEFAULT 1",
        "status VARCHAR(16) NOT NULL",
        "CHECK (status IN ('unresolved', 'resolved'))",
        "PRIMARY KEY (conflict_identity)",
        "row_conflicts_insert_guard",
        "row_conflicts_update_guard",
    ] {
        assert!(target.contains(field), "target fixture missing {field}");
    }
}

#[test]
fn source_based_harness_rebuilds_existing_binary_but_explicit_binary_does_not() {
    let script = harness_script();
    let code = format!(
        r#"
import importlib.util
import pathlib
import sys
import tempfile

script = pathlib.Path(r'{script}')
spec = importlib.util.spec_from_file_location('cdc_harness_freshness', script)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
repo = pathlib.Path(tempfile.mkdtemp())
binary = repo / 'target/debug/mariadb-mysql-cdc'
binary.parent.mkdir(parents=True)
binary.write_text('existing')
calls = []
module.run = lambda command, cwd=None, **kwargs: calls.append((command, cwd))
source_harness = module.Harness(repo, None)
assert source_harness._stream_binary(None) == binary
assert calls == [(['cargo', 'build', '--bin', 'mariadb-mysql-cdc'], repo)]
calls.clear()
explicit_harness = module.Harness(repo, binary)
assert explicit_harness._stream_binary(None) == binary
assert calls == []
"#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("check harness binary freshness");
    assert!(
        output.status.success(),
        "freshness check failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn recovery_scenarios_are_executable_and_use_failpoint_binary() {
    let script = fs::read_to_string(harness_script()).expect("read integration harness");
    for scenario in [
        "prepare-failure",
        "post-ddl-pre-applied",
        "applied-pre-checkpoint",
        "checkpoint-transaction",
    ] {
        assert!(script.contains(&format!("ScenarioSpec(\"{scenario}\", True)")));
    }
    for scenario in ["source-connection-loss", "target-connection-loss"] {
        assert!(script.contains(&format!("ScenarioSpec(\"{scenario}\", True)")));
    }
    assert!(script.contains("self.run_recovery_scenario(scenario)"));
    assert!(script.contains("self.run_connection_loss_scenario(scenario)"));
    assert!(script.contains("ScenarioSpec(\"replace-divergent-pk\", True)"));
    assert!(script.contains("self.run_replace_divergent_pk()"));
    assert!(script.contains("ScenarioSpec(\"row-conflict-rollback\", True)"));
    assert!(script.contains("self.run_row_conflict_rollback()"));
    assert!(script.contains("--features"));
    assert!(script.contains("integration-failpoints"));
    assert!(script.contains("--integration-failpoint"));
}

#[test]
fn failpoints_are_absent_from_default_build_surface_and_not_env_backdoors() {
    let binary = env!("CARGO_BIN_EXE_mariadb-mysql-cdc");
    let help = Command::new(binary)
        .arg("--help")
        .output()
        .expect("run default CDC binary help");
    assert!(help.status.success(), "default binary help failed");
    assert!(!String::from_utf8_lossy(&help.stdout).contains("--integration-failpoint"));

    let invalid_option = Command::new(binary)
        .args([
            "stream-binlog",
            "--integration-failpoint",
            "prepare-failure",
        ])
        .output()
        .expect("run default CDC binary with failpoint option");
    assert!(!invalid_option.status.success());
    let invalid_option_error = String::from_utf8_lossy(&invalid_option.stderr);
    assert!(invalid_option_error.contains("--integration-failpoint"));

    let clean_plan = Command::new(binary)
        .arg("plan")
        .output()
        .expect("run default CDC binary plan");
    let env_plan = Command::new(binary)
        .arg("plan")
        .env("CDC_FAILPOINT", "source-connection-loss")
        .output()
        .expect("run default CDC binary plan with failpoint environment variable");
    assert_eq!(clean_plan.status, env_plan.status);
    assert_eq!(clean_plan.stdout, env_plan.stdout);
    assert_eq!(clean_plan.stderr, env_plan.stderr);
}

#[test]
fn recovery_specs_assert_state_evidence_not_only_process_exit() {
    let script = fs::read_to_string(harness_script()).expect("read integration harness");
    for marker in [
        "ddl_replay_journal",
        "stream_checkpoint",
        "SHOW INDEX",
        "restart",
        "overtake",
        "convergence",
        "blocking",
    ] {
        assert!(
            script
                .to_ascii_lowercase()
                .contains(&marker.to_ascii_lowercase()),
            "missing recovery assertion marker: {marker}"
        );
    }
}

fn section<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
    let start_index = source.find(start).expect("section start");
    let end_index = source[start_index..]
        .find(end)
        .map(|index| start_index + index)
        .expect("section end");
    &source[start_index..end_index]
}
