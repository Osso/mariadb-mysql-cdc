pub mod canonical_foreign_key;
pub mod catchup;
pub mod checkpoint;
pub mod checksum;
pub mod conflict_ledger;
pub mod cutover;
pub mod inventory;
pub mod live;
mod lost_binlog_recovery;
mod lost_binlog_recovery_store;
pub mod mysql_client;
pub mod mysql_config;
pub mod mysql_snapshot;
pub mod mysql_support;
mod primary_key_ordering;
mod probe;
pub mod rehearsal;
pub mod row;
pub mod snapshot;
mod snapshot_ranges;
mod sql_type;
pub mod statement;
pub mod stream_checkpoint;
mod sync;
mod sync_cli;
mod sync_schema;
pub mod table_catalog;
pub mod table_sync;
pub mod target;
pub mod targeted_conflict_resolution;
pub mod validation;

use std::env;

const USAGE: &str = "\
mariadb-mysql-cdc

Usage:
  mariadb-mysql-cdc plan
  mariadb-mysql-cdc probe --host HOST --user USER --password-env ENV [options]
  mariadb-mysql-cdc sync --source-host HOST --source-user USER --source-password-env ENV --source-database DB --target-host HOST --target-user USER --target-password-env ENV --target-database DB --target-tls-ca-file PATH --table TABLE [--table TABLE ...] (--run-id ID | --run-id-prefix PREFIX) [options]
  mariadb-mysql-cdc table-catalog --source-host HOST --source-user USER --source-password-env ENV --source-database DB --target-host HOST --target-user USER --target-password-env ENV --target-database DB --target-tls-ca-file PATH --syncable-output PATH --non-syncable-output PATH
  mariadb-mysql-cdc sync-catalog --source-host HOST --source-user USER --source-password-env ENV --source-database DB --target-host HOST --target-user USER --target-password-env ENV --target-database DB --target-tls-ca-file PATH --catalog PATH --run-id-prefix PREFIX [options]
  mariadb-mysql-cdc recover-lost-binlog --authorization-file PATH --source-host HOST --source-user USER --source-password-env ENV --source-database DB --source-identity ID --target-host HOST --target-user USER --target-password-env ENV --target-database DB
  mariadb-mysql-cdc resync-stream --source-host HOST --source-user USER --source-password-env ENV --source-database DB --source-identity NEW_ID --target-host HOST --target-user USER --target-password-env ENV --target-database DB [--parallelism WORKERS]
  mariadb-mysql-cdc resolve-comics-releases-views-conflicts --source-host HOST --source-user USER --source-password-env ENV --source-database DB --source-identity ID --target-host HOST --target-user USER --target-password-env ENV --target-database DB --target-tls-ca-file PATH --run-id ID [--batch-size ROWS]
  mariadb-mysql-cdc apply-binlog --source-host HOST --source-user USER --source-password-env ENV --target-host HOST --target-user USER --target-password-env ENV --target-database DB [options]
  mariadb-mysql-cdc stream-binlog --source-host HOST --source-user USER --source-password-env ENV --target-host HOST --target-user USER --target-password-env ENV --target-database DB [options]

Commands:
  plan    Print the current migration tool design.
  probe   Read source binlog coordinates and classify MariaDB binlog events.
  sync    Synchronize target schemas and table rows from source.
  table-catalog
          Write deterministic syncable and non-syncable table catalogs ordered by estimated source rows.
  sync-catalog
          Apply a syncable table catalog through unified staged synchronization.
  recover-lost-binlog
          Execute one authorization-file-scoped lost-binlog recovery with a source-consistent full-scope repair and immutable audit record.
  resolve-comics-releases-views-conflicts
          Verify exact unresolved child and referenced UTM rows, then resolve only equal conflicts.
  apply-binlog
          Read remote MariaDB binlog text and apply compatible statements.
  stream-binlog
          Stream native MariaDB ROW/FULL binlog events with transactional target checkpoints and durable DDL translation barriers.

Probe options:
  --host HOST                 MariaDB source host.
  --port PORT                 MariaDB source port. Defaults to 3306.
  --user USER                 MariaDB replication user.
  --password-env ENV          Environment variable containing the password.
  --binlog-file FILE          Override SHOW MASTER STATUS binlog file.
  --start-position POSITION   Override SHOW MASTER STATUS position.
  --stop-position POSITION    Stop reading at a binlog position.
Sync options:
  --source-host HOST              MariaDB source host.
  --source-port PORT              MariaDB source port. Defaults to 3306.
  --source-user USER              MariaDB source user.
  --source-password-env ENV       Environment variable containing source password.
  --source-database DB            MariaDB source database.
  --target-host HOST              MySQL target host.
  --target-port PORT              MySQL target port. Defaults to 3306.
  --target-user USER              MySQL target user.
  --target-password-env ENV       Environment variable containing target password.
  --target-database DB            MySQL target database.
  --target-tls-ca-file PATH       Target CA certificate bundle.
  --table TABLE                   Source-authoritative table; repeat for multiple tables.
  --chunk-size ROWS               Rows per locked chunk. Defaults to 1000.
  --parallelism WORKERS           Concurrent table workers. Defaults to 1.
  --progress-table TABLE          Staged progress table. Defaults to cdc.sync_runs.
  --run-id ID                     Exact immutable run identity.
  --run-id-prefix PREFIX          Deterministic immutable run identity namespace.

Apply options:
  --source-host HOST              MariaDB source host.
  --source-port PORT              MariaDB source port. Defaults to 3306.
  --source-user USER              MariaDB replication user.
  --source-password-env ENV       Environment variable containing source password.
  --source-database DB            Limit source binlog statements to this database.
  --source-identity ID            Required immutable source-incarnation ID; change after source rebuild/reset.
  --binlog-file FILE              Source binlog file.
  --start-position POSITION       Source binlog start position.
  --stop-position POSITION        Stop reading at source binlog position.
  --target-host HOST              MySQL target host.
  --target-port PORT              MySQL target port. Defaults to 3306.
  --target-user USER              MySQL target user.
  --target-password-env ENV       Environment variable containing target password.
  --target-database DB            MySQL target database.
  --target-tls-ca-file PATH        Target CA certificate bundle. Defaults to /etc/mariadb-mysql-cdc/do-ca.pem.
  --insert-conflict-policy POLICY Statement/snapshot INSERT policy: error, ignore-duplicate, or replace-divergent-pk. Native ROW streaming is fixed.
  --max-reconnects COUNT          Stream reconnect cap. Defaults to 12.
  --reconnect-forever BOOL        Ignore reconnect cap for transient source loss. Defaults to false.
  --target-parallel-transactions COUNT
                                  Submit complete target transactions concurrently. Defaults to 1 (serial).
  --stop-never-slave-server-id ID MariaDB --stop-never slave server_id. Generated when omitted.

Sync catalog options:
  --run-id-prefix PREFIX          Fresh immutable identity namespace for this catalog attempt.

";

fn main() {
    let mut args = env::args();
    let _binary = args.next();
    let command = args.next();

    match command.as_deref() {
        Some("plan") => print_plan(),
        Some("probe") => run_probe_command(args.collect()),
        Some("sync") => sync_cli::run_sync_command(args.collect(), USAGE),
        Some("table-catalog") => table_catalog::run_table_catalog_command(args.collect(), USAGE),
        Some("sync-catalog") => table_catalog::run_sync_catalog_command(args.collect(), USAGE),
        Some("recover-lost-binlog") => run_recover_lost_binlog_command(args.collect()),
        Some("resync-stream") => run_resync_stream_command(args.collect()),
        Some("resolve-comics-releases-views-conflicts") => {
            run_targeted_conflict_resolution_command(args.collect())
        }
        Some("apply-binlog") => run_apply_binlog_command(args.collect()),
        Some("stream-binlog") => run_stream_binlog_command(args.collect()),
        Some("-h" | "--help") | None => print!("{USAGE}"),
        Some(other) => exit_unknown_command(other),
    }
}

fn run_targeted_conflict_resolution_command(args: Vec<String>) {
    targeted_conflict_resolution::run_targeted_conflict_resolution_command(args, USAGE)
}

fn exit_unknown_command(command: &str) {
    eprintln!("unknown command: {command}\n\n{USAGE}");
    std::process::exit(2);
}

fn run_resync_stream_command(mut args: Vec<String>) {
    let parallelism = match take_optional_nonzero_usize(&mut args, "--parallelism", 1) {
        Ok(parallelism) => parallelism,
        Err(error) => {
            eprintln!("{error}\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    args.extend([
        "--binlog-file".to_string(),
        "resync-boundary".to_string(),
        "--start-position".to_string(),
        "4".to_string(),
    ]);
    let apply = match parse_apply_binlog_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    let source_database = apply
        .source
        .database
        .clone()
        .expect("validated source database");
    let config = resync_config_from_apply(apply, source_database, parallelism);
    match lost_binlog_recovery::run_resync_stream(&config) {
        Ok(report) => println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("resync report JSON")
        ),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn resync_config_from_apply(
    apply: live::ApplyBinlogConfig,
    source_database: String,
    parallelism: usize,
) -> lost_binlog_recovery::ResyncStreamConfig {
    lost_binlog_recovery::ResyncStreamConfig {
        source: crate::mysql_config::MySqlConnectionConfig {
            host: apply.source.host,
            port: apply.source.port,
            user: apply.source.user,
            password: apply.source.password,
            database: source_database,
        },
        source_identity: apply.source_identity,
        target: apply.target,
        checkpoint_table: apply.checkpoint_table,
        progress_table: sync::DEFAULT_SYNC_PROGRESS_TABLE.to_string(),
        chunk_size: 10_000,
        parallelism,
    }
}

fn run_recover_lost_binlog_command(args: Vec<String>) {
    let config = match parse_recover_lost_binlog_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    match lost_binlog_recovery::run_recover_lost_binlog(&config) {
        Ok(report) => print_recovery_report(&report),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn parse_recover_lost_binlog_config(
    args: Vec<String>,
) -> Result<lost_binlog_recovery::RecoverLostBinlogConfig, String> {
    let (authorization_file, apply_args) = take_required_option(args, "--authorization-file")?;
    let apply = parse_apply_binlog_config(apply_args)?;
    let source_database = apply
        .source
        .database
        .clone()
        .ok_or_else(|| "source database is required".to_string())?;
    Ok(recovery_config_from_apply(
        apply,
        authorization_file,
        source_database,
    ))
}

fn recovery_config_from_apply(
    apply: live::ApplyBinlogConfig,
    authorization_file: String,
    source_database: String,
) -> lost_binlog_recovery::RecoverLostBinlogConfig {
    lost_binlog_recovery::RecoverLostBinlogConfig {
        source: crate::mysql_config::MySqlConnectionConfig {
            host: apply.source.host,
            port: apply.source.port,
            user: apply.source.user,
            password: apply.source.password,
            database: source_database,
        },
        source_identity: apply.source_identity,
        target: apply.target,
        authorization_file: authorization_file.into(),
        checkpoint_table: apply.checkpoint_table,
        journal_table: "cdc.ddl_replay_journal".to_string(),
        recovery_table: lost_binlog_recovery_store::DEFAULT_RECOVERY_TABLE.to_string(),
        progress_table: sync::DEFAULT_SYNC_PROGRESS_TABLE.to_string(),
        chunk_size: 10_000,
    }
}

fn print_recovery_report(report: &lost_binlog_recovery::RecoverLostBinlogReport) {
    println!(
        "{}",
        serde_json::to_string_pretty(report).expect("recovery report JSON")
    );
}

fn take_optional_nonzero_usize(
    args: &mut Vec<String>,
    option: &str,
    default: usize,
) -> Result<usize, String> {
    let mut remaining = Vec::with_capacity(args.len());
    let mut value = default;
    let mut index = 0;
    while index < args.len() {
        if args[index] == option {
            let raw = args
                .get(index + 1)
                .ok_or_else(|| format!("{option} needs a value"))?;
            value = parse_nonzero_usize(option, raw)?;
            index += 2;
        } else {
            remaining.push(args[index].clone());
            index += 1;
        }
    }
    *args = remaining;
    Ok(value)
}

fn take_required_option(
    args: Vec<String>,
    required_flag: &str,
) -> Result<(String, Vec<String>), String> {
    let mut remaining = Vec::new();
    let mut value = None;
    let mut arguments = args.into_iter();
    while let Some(argument) = arguments.next() {
        if argument == required_flag {
            if value.is_some() {
                return Err(format!("{required_flag} may only be supplied once"));
            }
            value = Some(
                arguments
                    .next()
                    .ok_or_else(|| format!("missing value for {required_flag}"))?,
            );
        } else {
            remaining.push(argument);
        }
    }
    value
        .map(|value| (value, remaining))
        .ok_or_else(|| format!("{required_flag} is required"))
}

fn run_stream_binlog_command(args: Vec<String>) {
    let config = match parse_apply_binlog_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{USAGE}");
            std::process::exit(2);
        }
    };

    if let Err(error) = live::stream_remote_binlog(&config) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run_apply_binlog_command(args: Vec<String>) {
    let config = match parse_apply_binlog_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{USAGE}");
            std::process::exit(2);
        }
    };

    match live::apply_remote_binlog(&config) {
        Ok(report) => {
            println!(
                "Applied {} statements; quarantined {} statements",
                report.applied_statements, report.quarantined_statements
            );
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn print_plan() {
    println!(
        "\
Goal: migrate MariaDB to MySQL-compatible targets with minimal downtime.

Constraints:
- Require and preflight MariaDB binlog_format=ROW with binlog_row_image=FULL for production streaming.
- Do not require DigitalOcean Managed MySQL to serve traffic before rehearsals pass.
- Treat incompatible SQL as migration bugs to capture and fix before cutover.

Synchronization:
1. Converge prerequisite target schema from source evidence.
2. Synchronize source-authoritative rows in target-WRITE-locked chunks.
3. Converge final constraints and persist durable stage progress.
4. Stream source transactions with ordered target commits and checkpoints.
"
    );
}

fn run_probe_command(args: Vec<String>) {
    let config = match parse_probe_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    let mut runner = probe::ProcessRunner;

    match probe::run_probe(&config, &mut runner) {
        Ok(report) => probe::print_report(&report),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn parse_probe_config(args: Vec<String>) -> Result<probe::ProbeConfig, String> {
    let mut config = probe::ProbeConfig::default();
    let mut index = 0;

    while index < args.len() {
        let flag = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value"))?;

        match flag.as_str() {
            "--host" => config.host = value.clone(),
            "--port" => config.port = parse_u16(flag, value)?,
            "--user" => config.user = value.clone(),
            "--password-env" => config.password = read_env_password(value)?,
            "--binlog-file" => config.binlog_file = Some(value.clone()),
            "--start-position" => config.start_position = Some(parse_u64(flag, value)?),
            "--stop-position" => config.stop_position = Some(parse_u64(flag, value)?),
            other => return Err(format!("unknown probe option: {other}")),
        }

        index += 2;
    }

    Ok(config)
}

fn parse_apply_binlog_config(args: Vec<String>) -> Result<live::ApplyBinlogConfig, String> {
    let mut config = live::ApplyBinlogConfig::default();
    config.source.start_position = 0;
    let mut index = 0;

    while index < args.len() {
        let flag = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value"))?;

        apply_binlog_option(&mut config, flag, value)?;

        index += 2;
    }

    config.validate().map_err(|error| error.to_string())?;
    Ok(config)
}

fn apply_binlog_option(
    config: &mut live::ApplyBinlogConfig,
    flag: &str,
    value: &str,
) -> Result<(), String> {
    if apply_source_option(&mut config.source, flag, value)? {
        return Ok(());
    }
    if apply_target_option(&mut config.target, flag, value)? {
        return Ok(());
    }
    if apply_binlog_runtime_option(config, flag, value)? {
        return Ok(());
    }
    Err(format!("unknown apply-binlog option: {flag}"))
}

fn apply_binlog_runtime_option(
    config: &mut live::ApplyBinlogConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    if apply_binlog_identity_option(config, flag, value)? {
        return Ok(true);
    }
    if apply_binlog_reconnect_option(config, flag, value)? {
        return Ok(true);
    }
    if apply_binlog_transaction_option(config, flag, value)? {
        return Ok(true);
    }
    if apply_binlog_source_server_option(config, flag, value)? {
        return Ok(true);
    }
    Ok(false)
}

fn apply_binlog_identity_option(
    config: &mut live::ApplyBinlogConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--source-identity" => config.source_identity = value.to_string(),
        "--checkpoint-table" => config.checkpoint_table = value.to_string(),
        _ => return Ok(false),
    }

    Ok(true)
}

fn apply_binlog_reconnect_option(
    config: &mut live::ApplyBinlogConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--max-reconnects" => config.max_reconnects = parse_u32(flag, value)?,
        "--reconnect-forever" => config.reconnect_forever = parse_bool(flag, value)?,
        _ => return Ok(false),
    }

    Ok(true)
}

fn apply_binlog_transaction_option(
    config: &mut live::ApplyBinlogConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--target-transaction-group-size" => {
            config.target_transaction_group_size = parse_nonzero_usize(flag, value)?;
        }
        "--target-transaction-group-timeout-ms" => {
            config.target_transaction_group_timeout_ms = parse_u64(flag, value)?;
        }
        "--target-parallel-transactions" => {
            config.target_parallel_transactions = parse_nonzero_usize(flag, value)?;
        }
        #[cfg(feature = "integration-failpoints")]
        "--integration-failpoint" => {
            config.integration_failpoint = Some(live::IntegrationFailpoint::parse(value)?);
        }
        _ => return Ok(false),
    }

    Ok(true)
}

fn apply_binlog_source_server_option(
    config: &mut live::ApplyBinlogConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--stop-never-slave-server-id" => {
            config.source.stop_never_slave_server_id = Some(parse_nonzero_u32(flag, value)?);
        }
        _ => return Ok(false),
    }

    Ok(true)
}

fn apply_source_option(
    source: &mut live::SourceBinlogConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--source-host" => source.host = value.to_string(),
        "--source-port" => source.port = parse_u16(flag, value)?,
        "--source-user" => source.user = value.to_string(),
        "--source-password-env" => source.password = read_env_password(value)?,
        "--source-database" => source.database = Some(value.to_string()),
        "--binlog-file" => source.binlog_file = value.to_string(),
        "--start-position" => source.start_position = parse_u64(flag, value)?,
        "--stop-position" => source.stop_position = Some(parse_u64(flag, value)?),
        _ => return Ok(false),
    }

    Ok(true)
}

fn apply_target_option(
    target: &mut live::TargetMySqlConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--target-host" => target.host = value.to_string(),
        "--target-port" => target.port = parse_u16(flag, value)?,
        "--target-user" => target.user = value.to_string(),
        "--target-password-env" => target.password = read_env_password(value)?,
        "--target-database" => target.database = value.to_string(),
        "--target-tls-ca-file" => target.tls_ca_file = value.to_string(),
        "--insert-conflict-policy" => target.insert_conflict_policy = parse_insert_policy(value)?,
        _ => return Ok(false),
    }

    Ok(true)
}

pub(crate) fn parse_insert_policy(value: &str) -> Result<live::InsertConflictPolicy, String> {
    match value {
        "error" => Ok(live::InsertConflictPolicy::Error),
        "ignore-duplicate" => Ok(live::InsertConflictPolicy::IgnoreDuplicate),
        "replace-divergent-pk" => Ok(live::InsertConflictPolicy::ReplaceDivergentPk),
        other => Err(format!("unknown insert conflict policy: {other}")),
    }
}

pub(crate) fn read_env_password(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("{name} is not set"))
}

pub(crate) fn parse_u16(flag: &str, value: &str) -> Result<u16, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} must be an integer"))
}

fn parse_u64(flag: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} must be an integer"))
}

fn parse_u32(flag: &str, value: &str) -> Result<u32, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} must be an integer"))
}

fn parse_nonzero_usize(flag: &str, value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be an integer"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be greater than zero"));
    }
    Ok(parsed)
}

fn parse_nonzero_u32(flag: &str, value: &str) -> Result<u32, String> {
    let parsed = parse_u32(flag, value)?;
    if parsed == 0 {
        return Err(format!("{flag} must be greater than zero"));
    }

    Ok(parsed)
}

pub(crate) fn parse_bool(flag: &str, value: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{flag} must be true or false")),
    }
}

pub(crate) fn parse_usize(flag: &str, value: &str) -> Result<usize, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} must be an integer"))
}

#[cfg(test)]
mod tests;
