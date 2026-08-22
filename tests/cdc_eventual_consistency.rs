use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn harness_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/cdc-integration-harness.py")
}

fn run_harness_scenario(scenario: &str) {
    let output = Command::new("python3")
        .arg(harness_script())
        .arg("--scenario")
        .arg(scenario)
        .output()
        .expect("run CDC integration harness");
    assert!(
        output.status.success(),
        "integration harness failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
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
fn real_live_missing_fk_parent_is_copied_before_child_retry() {
    run_harness_scenario("missing-fk-parent-auto-insert");
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_serial_target_repairs_nested_missing_fk_parents() {
    run_harness_scenario("missing-fk-nested-parent-auto-insert");
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_unified_sync_tls_harness_smoke() {
    run_harness_scenario("sync-tls");
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_unified_sync_composite_enum_primary_key_uses_enum_order() {
    run_harness_scenario("sync-composite-enum-primary-key");
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_unified_sync_fk_parent_insert_converges() {
    run_harness_scenario("sync-fk-parent-insert");
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_unified_sync_fk_parent_update_converges() {
    run_harness_scenario("sync-fk-parent-update");
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_unified_sync_reconciles_stale_unique_owner() {
    run_harness_scenario("sync-fk-parent-stale-unique-owner");
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_unified_sync_unique_owner_repair_rolls_back_and_resumes() {
    run_harness_scenario("sync-unique-owner-rollback-resume");
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_unified_sync_wide_update_converges() {
    run_harness_scenario("sync-wide-update");
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_unified_sync_resumes_durable_progress() {
    run_harness_scenario("sync-resume");
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_unified_sync_progress_uses_least_privilege_identity() {
    run_harness_scenario("sync-progress-least-privilege");
}

#[test]
#[ignore = "starts MariaDB 11.4 and MySQL 8 Docker containers"]
fn real_writable_metadata_keeps_default_generated_columns() {
    let output = Command::new("python3")
        .arg(harness_script())
        .arg("--scenario")
        .arg("writable-column-generated-metadata")
        .output()
        .expect("run writable-column metadata harness");

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
fn source_harness_stream_account_allows_plaintext_transport() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/cdc-harness-source-bootstrap.sql");
    let source_bootstrap = fs::read_to_string(fixture).expect("read source bootstrap fixture");
    assert!(!source_bootstrap.contains("REQUIRE SSL"));
}

#[test]
fn sync_harness_uses_current_cli_flags_and_target_ca() {
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

harness = module.Harness.__new__(module.Harness)
harness.source = module.Endpoint('source', 3307)
harness.target = module.Endpoint('target', 3308)
harness.ca_file = pathlib.Path('/tmp/shared-ca.pem')
args = harness._sync_args(
    pathlib.Path('/tmp/cdc'),
    tables=['parents', 'children'],
    run_id='sync-test',
    chunk_size=500,
    parallelism=4,
    target_ca_file=pathlib.Path('/tmp/target-ca.pem'),
)
assert args[1] == 'sync'
assert '--source-tls-ca-file' not in args
assert args[args.index('--target-tls-ca-file') + 1] == '/tmp/target-ca.pem'
assert args[args.index('--progress-table') + 1] == 'cdc.sync_runs'
assert args[args.index('--run-id') + 1] == 'sync-test'
assert args[args.index('--chunk-size') + 1] == '500'
assert args[args.index('--parallelism') + 1] == '4'
assert [args[index + 1] for index, value in enumerate(args) if value == '--table'] == ['parents', 'children']
"#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("check unified sync CLI arguments");
    assert!(
        output.status.success(),
        "unified sync arguments failed:
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
fn unified_sync_scenarios_are_executable_and_dispatched() {
    let script = harness_script();
    let code = format!(
        r#"
import importlib.util
import pathlib
import sys

script = pathlib.Path(r'{script}')
spec = importlib.util.spec_from_file_location('cdc_harness_dispatch', script)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

harness = module.Harness.__new__(module.Harness)
calls = []
harness.prepare = lambda: calls.append('prepare')
harness.run_sync_tls = lambda: calls.append('sync-tls')
harness.run_sync_composite_enum_primary_key = lambda: calls.append('sync-composite-enum-primary-key')
harness.run_sync_fk_parent_convergence = lambda update_existing_child=False: calls.append(
    'sync-fk-parent-update' if update_existing_child else 'sync-fk-parent-insert'
)
harness.run_sync_fk_parent_stale_unique_owner = lambda: calls.append('sync-fk-parent-stale-unique-owner')
harness.run_sync_unique_owner_rollback_resume = lambda: calls.append('sync-unique-owner-rollback-resume')
harness.run_sync_wide_update = lambda: calls.append('sync-wide-update')
harness.run_sync_resume = lambda: calls.append('sync-resume')
harness.run_sync_progress_least_privilege = lambda: calls.append('sync-progress-least-privilege')

for scenario in (
    'sync-tls',
    'sync-composite-enum-primary-key',
    'sync-fk-parent-insert',
    'sync-fk-parent-update',
    'sync-fk-parent-stale-unique-owner',
    'sync-unique-owner-rollback-resume',
    'sync-wide-update',
    'sync-resume',
    'sync-progress-least-privilege',
):
    calls.clear()
    harness.run_scenario(scenario)
    assert calls == ['prepare', scenario], (scenario, calls)
"#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("exercise unified sync scenario dispatch");
    assert!(
        output.status.success(),
        "unified sync dispatch failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
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
        "sync-tls",
        "sync-composite-enum-primary-key",
        "sync-fk-parent-insert",
        "sync-fk-parent-update",
        "sync-fk-parent-stale-unique-owner",
        "sync-wide-update",
        "sync-resume",
        "sync-progress-least-privilege",
        "missing-fk-nested-parent-auto-insert",
        "prepare-failure",
        "post-ddl-pre-applied",
        "applied-pre-checkpoint",
        "checkpoint-transaction",
        "source-connection-loss",
        "target-connection-loss",
    ] {
        assert!(
            listed
                .lines()
                .any(|line| line == format!("{scenario}\texecutable"))
        );
    }
    for removed_prefix in [
        "catchup-snapshot",
        "sync-table",
        "repair-",
        "parallel-target-transactions",
    ] {
        assert!(!listed.lines().any(|line| line.starts_with(removed_prefix)));
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
    let runtime = section(&script, "    def _stream_args", "    def run_stream");
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
    assert!(runtime.contains("LIVE_TARGET_USER"));
    assert!(!runtime.contains("SYNC_TARGET_USER"));
    assert!(runtime.contains("SOURCE_USER"));
    assert!(!runtime.contains("--source-tls-ca-file"));
    assert!(runtime.contains("--target-tls-ca-file"));
    assert!(!runtime.contains("--insert-conflict-policy"));
    assert!(!runtime.contains("--integration-logical-"));
    assert!(!runtime.contains("/etc/mariadb-mysql-cdc/do-ca.pem"));
}

#[test]
fn harness_separates_live_and_sync_target_identities() {
    let script = harness_script();
    let code = format!(
        r#"
import importlib.util
import pathlib
import sys

script = pathlib.Path(r'{script}')
spec = importlib.util.spec_from_file_location('cdc_harness_identity', script)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

harness = module.Harness.__new__(module.Harness)
harness.source = module.Endpoint('source', 3307)
harness.target = module.Endpoint('target', 3308)
harness.ca_file = pathlib.Path('/tmp/target-ca.pem')
live = harness._stream_args(
    pathlib.Path('/tmp/cdc'),
    module.Coordinate('mysql-bin.000001', 4),
    None,
    None,
    0,
)
sync = harness._sync_args(
    pathlib.Path('/tmp/cdc'),
    tables=['accounts'],
    run_id='identity-test',
)
assert live[live.index('--target-user') + 1] == module.LIVE_TARGET_USER
assert sync[sync.index('--target-user') + 1] == module.SYNC_TARGET_USER
assert module.LIVE_TARGET_USER != module.SYNC_TARGET_USER
"#,
        script = script.display()
    );
    let output = Command::new("python3")
        .args(["-c", &code])
        .output()
        .expect("check live and sync target identities");
    assert!(
        output.status.success(),
        "target identity separation failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn live_harness_covers_source_authoritative_duplicate_inserts() {
    let script = fs::read_to_string(harness_script()).expect("read integration harness");

    assert!(script.contains("ScenarioSpec(\"insert-duplicate-idempotent\", True)"));
    assert!(script.contains("self.run_insert_duplicate_idempotent()"));
}

#[test]
fn live_harness_excludes_retired_conflict_paths() {
    let script = fs::read_to_string(harness_script()).expect("read integration harness");
    for scenario in [
        "home-feed-card-parent-recovery",
        "superseded-release-visibility-recovery",
        "generic-fk-missing-parent",
        "generic-fk-missing-parent-binary",
        "generic-fk-superseded-attribute",
        "generic-fk-source-parent-mismatch",
        "generic-fk-restrict-rejected",
        "superseded-users-recovery",
        "missing-conflict-trigger",
        "missing-conflict-table",
        "wrong-conflict-schema",
        "missing-conflict-grant",
        "broad-conflict-grant",
        "replace-divergent-pk",
        "row-conflict-rollback",
        "row-conflict-indexed-resolution",
        "durable-row-conflict-retry",
    ] {
        assert!(!script.contains(&format!("ScenarioSpec(\"{scenario}\", True)")));
    }
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
    assert!(!target.contains("GRANT EXECUTE ON PROCEDURE cdc.row_conflicts_trigger_inventory"));
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
    for user in ["cdc_stream", "cdc_sync"] {
        assert!(target.contains(user));
        assert!(target.contains(&format!("ON globalcomix.* TO '{user}'@'%'")));
    }
    assert!(target.contains("ON cdc.stream_checkpoint TO 'cdc_stream'@'%'"));
    assert!(target.contains("ON cdc.ddl_replay_journal TO 'cdc_stream'@'%'"));
    assert!(target.contains("GRANT CREATE ON cdc.* TO 'cdc_sync'@'%'"));
    assert!(target.contains("ON cdc.sync_runs TO 'cdc_sync'@'%'"));
    assert!(!target.contains("cdc_repair"));
    assert!(!target.contains("ON cdc.row_conflicts TO 'cdc_stream'@'%'"));
    assert!(!target.contains("ON cdc.row_conflicts TO 'cdc_sync'@'%'"));
    assert!(!target.contains(
        "GRANT EXECUTE ON PROCEDURE cdc.row_conflicts_trigger_inventory TO 'cdc_stream'@'%'"
    ));
    assert!(!target.contains(
        "GRANT EXECUTE ON PROCEDURE cdc.row_conflicts_trigger_inventory TO 'cdc_sync'@'%'"
    ));
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
fn runtime_grant_docs_remove_legacy_stream_control_plane_access() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let runtime_grants = fs::read_to_string(root.join("docs/ddl-runtime-grants.sql.example"))
        .expect("read runtime grants example");
    let control_plane = fs::read_to_string(root.join("docs/ddl-control-plane-bootstrap.sql"))
        .expect("read control-plane bootstrap");

    for sql in [runtime_grants, control_plane] {
        let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            !normalized
                .contains("GRANT SELECT, INSERT, UPDATE ON cdc.row_conflicts TO 'cdc_stream'@'%';")
        );
        assert!(!normalized.contains(
            "GRANT EXECUTE ON PROCEDURE cdc.row_conflicts_trigger_inventory TO 'cdc_stream'@'%';"
        ));
    }

    let migration =
        fs::read_to_string(root.join("docs/live-stream-runtime-grants-migration-20260818.sql"))
            .expect("read live-stream runtime grants migration");
    let normalized = migration.split_whitespace().collect::<Vec<_>>().join(" ");
    for revoke in [
        "REVOKE SELECT, INSERT, UPDATE ON cdc.row_conflicts FROM 'cdc_stream'@'%';",
        "REVOKE EXECUTE ON PROCEDURE cdc.row_conflicts_trigger_inventory FROM 'cdc_stream'@'%';",
        "REVOKE SELECT, INSERT, UPDATE ON cdc.table_sync_runs FROM 'cdc_stream'@'%';",
    ] {
        assert!(
            normalized.contains(revoke),
            "missing migration statement: {revoke}"
        );
    }
    assert!(!normalized.contains("DROP TABLE"));
    assert!(!normalized.contains("DROP PROCEDURE"));
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
assert source_harness._sync_binary() == binary
assert calls == [(['cargo', 'build', '--bin', 'mariadb-mysql-cdc'], repo)]
calls.clear()
explicit_harness = module.Harness(repo, binary)
assert explicit_harness._stream_binary(None) == binary
assert explicit_harness._sync_binary() == binary
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
    assert!(script.contains("--features"));
    assert!(script.contains("integration-failpoints"));
    assert!(script.contains("--integration-failpoint"));
}

#[cfg(not(feature = "integration-failpoints"))]
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

#[test]
fn target_bootstrap_uses_exact_unified_sync_progress_contract() {
    let target = fs::read_to_string(&fixture_paths()[1]).expect("read target bootstrap fixture");
    let sync_application_grant = target
        .split(';')
        .find(|statement| statement.contains("ON globalcomix.* TO 'cdc_sync'@'%'"))
        .expect("cdc_sync application grant");
    assert!(sync_application_grant.contains("LOCK TABLES"));

    for required in [
        "CREATE USER IF NOT EXISTS 'cdc_sync'@'%'",
        "CREATE TABLE IF NOT EXISTS cdc.sync_runs",
        "run_id VARCHAR(128) NOT NULL",
        "stage VARCHAR(32) NOT NULL",
        "table_name VARCHAR(255) NOT NULL",
        "run_spec_json LONGTEXT NOT NULL",
        "last_primary_key_json TEXT NULL",
        "chunks BIGINT UNSIGNED NOT NULL DEFAULT 0",
        "rows_scanned BIGINT UNSIGNED NOT NULL DEFAULT 0",
        "inserts_applied BIGINT UNSIGNED NOT NULL DEFAULT 0",
        "updates_applied BIGINT UNSIGNED NOT NULL DEFAULT 0",
        "deletes_applied BIGINT UNSIGNED NOT NULL DEFAULT 0",
        "status VARCHAR(16) NOT NULL",
        "last_error TEXT NULL",
        "created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)",
        "updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)",
        "completed_at TIMESTAMP(6) NULL",
        "CHECK (stage IN ('prerequisite_schema', 'rows', 'final_constraints'))",
        "CHECK (status IN ('running', 'complete', 'error'))",
        "CHECK (JSON_VALID(run_spec_json))",
        "CHECK (last_primary_key_json IS NULL OR JSON_VALID(last_primary_key_json))",
        "PRIMARY KEY (run_id, stage, table_name)",
        "GRANT CREATE ON cdc.* TO 'cdc_sync'@'%'",
        "GRANT SELECT, INSERT, UPDATE ON cdc.stream_checkpoint TO 'cdc_sync'@'%'",
        "GRANT SELECT, INSERT, UPDATE ON cdc.sync_runs TO 'cdc_sync'@'%'",
    ] {
        assert!(
            target.contains(required),
            "target fixture missing {required}"
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
