pub mod catchup;
pub mod checkpoint;
pub mod checksum;
pub mod conflict_repair;
pub mod cutover;
pub mod drift_check;
pub mod inventory;
pub mod live;
mod lost_binlog_recovery;
mod lost_binlog_recovery_store;
pub mod mysql_client;
pub mod mysql_snapshot;
pub mod mysql_support;
mod probe;
pub mod rehearsal;
pub mod repair_drift;
pub mod row;
pub mod snapshot;
mod snapshot_ranges;
mod sql_type;
pub mod statement;
pub mod stream_checkpoint;
mod sync_cli;
mod sync_progress_cli;
mod sync_schema;
pub mod table_catalog;
pub mod table_sync;
pub mod target;
pub mod targeted_conflict_resolution;
pub mod validation;

use std::{env, path::PathBuf, time::Duration};

const USAGE: &str = "\
mariadb-mysql-cdc

Usage:
  mariadb-mysql-cdc plan
  mariadb-mysql-cdc probe --host HOST --user USER --password-env ENV [options]
  mariadb-mysql-cdc catchup-snapshot --source-host HOST --source-user USER --source-password-env ENV --source-database DB --target-host HOST --target-user USER --target-password-env ENV --target-database DB --progress-file PATH [options]
  mariadb-mysql-cdc catchup-progress --progress-file PATH
  mariadb-mysql-cdc sync-table --source-host HOST --source-user USER --source-password-env ENV --source-database DB --target-host HOST --target-user USER --target-password-env ENV --target-database DB --table TABLE --primary-key COLUMNS --columns COLUMNS --run-id ID [options]
  mariadb-mysql-cdc sync-progress --target-host HOST --target-user USER --target-password-env ENV --target-database DB [options]
  mariadb-mysql-cdc table-catalog --source-host HOST --source-user USER --source-password-env ENV --source-database DB --target-host HOST --target-user USER --target-password-env ENV --target-database DB --target-tls-ca-file PATH --syncable-output PATH --non-syncable-output PATH
  mariadb-mysql-cdc sync-catalog --source-host HOST --source-user USER --source-password-env ENV --source-database DB --target-host HOST --target-user USER --target-password-env ENV --target-database DB --target-tls-ca-file PATH --catalog PATH --run-id-prefix PREFIX [options]
  mariadb-mysql-cdc sync-schema --source-host HOST --source-user USER --source-password-env ENV --source-database DB --target-host HOST --target-user USER --target-password-env ENV --target-database DB --target-tls-ca-file PATH [--table TABLE ... | --catalog FILE | --all-tables true]
  mariadb-mysql-cdc drift-check --source-host HOST --source-user USER --source-password-env ENV --source-database DB --target-host HOST --target-user USER --target-password-env ENV --target-database DB [--table TABLE ...] [--content-check BOOL] [--chunk-size ROWS]
  mariadb-mysql-cdc repair-drift --source-host HOST --source-user USER --source-password-env ENV --source-database DB --target-host HOST --target-user USER --target-password-env ENV --target-database DB [options]
  mariadb-mysql-cdc recover-lost-binlog --authorization-file PATH --source-host HOST --source-user USER --source-password-env ENV --source-database DB --source-identity ID --target-host HOST --target-user USER --target-password-env ENV --target-database DB
  mariadb-mysql-cdc resync-stream --source-host HOST --source-user USER --source-password-env ENV --source-database DB --source-identity NEW_ID --target-host HOST --target-user USER --target-password-env ENV --target-database DB
  mariadb-mysql-cdc resolve-comics-releases-views-conflicts --source-host HOST --source-user USER --source-password-env ENV --source-database DB --source-identity ID --target-host HOST --target-user USER --target-password-env ENV --target-database DB --target-tls-ca-file PATH --run-id ID [--batch-size ROWS]
  mariadb-mysql-cdc apply-binlog --source-host HOST --source-user USER --source-password-env ENV --target-host HOST --target-user USER --target-password-env ENV --target-database DB [options]
  mariadb-mysql-cdc stream-binlog --source-host HOST --source-user USER --source-password-env ENV --target-host HOST --target-user USER --target-password-env ENV --target-database DB [options]

Commands:
  plan    Print the current migration tool design.
  probe   Read source binlog coordinates and classify MariaDB binlog events.
  catchup-snapshot
          Copy source rows into target in resumable primary-key chunks.
  catchup-progress
          Print catchup checkpoint progress.
  sync-table
          Compare one source/target table by primary-key chunks and optionally repair target gaps.
  sync-progress
          Print table sync progress, stream checkpoint, rates, and ETA when source counts are supplied.
  table-catalog
          Write deterministic syncable and non-syncable table catalogs ordered by estimated source rows.
  sync-catalog
          Apply a syncable table catalog with dependency ordering and sixteen total sync slots.
  sync-schema
          Converge selected target tables to the translated source schema; --all-tables true selects every source base table.
  drift-check
          Read-only source/target COUNT(*) drift check for selected tables, or all source base tables when no --table is supplied.
  repair-drift
          Inventory both endpoints, count-check source tables, and run bounded sync-table repairs only for drifted tables.
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
  --conflict-table TABLE           Durable row-conflict store. Defaults to cdc.row_conflicts.
  --insert-conflict-policy POLICY Replay INSERT conflict policy: error, ignore-duplicate, or replace-divergent-pk.
  --max-reconnects COUNT          Stream reconnect cap. Defaults to 12.
  --reconnect-forever BOOL        Ignore reconnect cap for transient source loss. Defaults to false.
  --stop-never-slave-server-id ID MariaDB --stop-never slave server_id. Generated when omitted.

Catchup snapshot options:
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
  --target-tls-ca-file PATH        Target CA certificate bundle. Defaults to /etc/mariadb-mysql-cdc/do-ca.pem.
  --progress-file PATH            Local fallback checkpoint file.
  --progress-table TABLE          Target checkpoint table. Defaults to cdc.table_sync_progress.

Sync table options:
  --target-tls-ca-file PATH        Target CA certificate bundle. Defaults to /etc/mariadb-mysql-cdc/do-ca.pem.
  --mode MODE                     apply (default), dry-run, or missing-primary-keys. dry-run reports the
                                  same chunk and row counts as apply and writes nothing.
  --progress-table TABLE          Target run-progress table. Defaults to cdc.table_sync_runs.
  --run-id ID                     Immutable repair run ID. Reuse only for interrupted runs; completed IDs are terminal.
  --start-after CSV               Primary-key lower bound for targeted sync-table repair.
  --end-at CSV                    Primary-key upper bound for targeted sync-table repair.
  --start-after-json JSON         Primary-key lower bound as JSON string array; use for values containing commas.
  --end-at-json JSON              Primary-key upper bound as JSON string array; use for values containing commas.
  --updated-at-column COLUMN      Source update timestamp column for update_time accelerator.
  --updated-since VALUE           Upsert source rows where updated-at column is >= VALUE. Does not delete orphans; run checksum validation after.

Sync progress repair-run options:
  --target-tls-ca-file PATH        Target CA certificate bundle. Defaults to /etc/mariadb-mysql-cdc/do-ca.pem.
  --progress-table TABLE          Use cdc.table_sync_runs to inspect sync-table repair runs.
  --run-id ID                     Filter the selected run-progress table by repair run ID.

  --chunk-size ROWS               Rows per chunk. Defaults to 10000.
  --throttle-ms MS                Sleep after each copied chunk. Defaults to 0.

Sync catalog options:
  --run-id-prefix PREFIX          Fresh immutable identity namespace for this catalog attempt.

Drift check options:
  --target-tls-ca-file PATH        Target CA certificate bundle. Defaults to /etc/mariadb-mysql-cdc/do-ca.pem.
  --table TABLE                   Limit drift check to a source table; repeat for multiple tables.
  --content-check BOOL             Run bounded content checks. Defaults to true.

Repair drift options:
  --target-tls-ca-file PATH        Target CA certificate bundle. Defaults to /etc/mariadb-mysql-cdc/do-ca.pem.
  --source-identity ID             Source incarnation ID for conflict resolution in apply mode.
  --table TABLE                    Limit repair to a source table; repeat for multiple tables.
  --parent-first TABLES            Comma-separated table order prefix; remaining tables sort lexically.
  --start-after CSV                Lower primary-key bound for the selected repair window.
  --end-at CSV                     Upper primary-key bound for the selected repair window.
  --start-after-json JSON          Lower primary-key bound as a JSON string array.
  --end-at-json JSON               Upper primary-key bound as a JSON string array.
  --content-check BOOL             Run bounded content checks when counts match. Defaults to true.
  --mode MODE                     dry-run (default) or apply.
  --conflict-reconcile-limit ROWS  Apply-only bounded pass over at most this many unresolved rows
                                  in the selected source/table scope. Read complete source and
                                  target rows by primary key; resolve only one-row complete
                                  equality with matching unresolved evidence. Missing, divergent,
                                  malformed, or ambiguous rows remain unresolved. Writes no
                                  target-table repairs and never reads or advances stream
                                  checkpoints; repeated passes are idempotent. Defaults to 0.
  --progress-table TABLE          Target run-progress table. Defaults to cdc.table_sync_runs.
  --run-id ID                     Reuse an interrupted repair run; plan hash must match.
  --run-id-prefix PREFIX          Prefix for the fresh run-scoped repair ID.
";

fn main() {
    let mut args = env::args();
    let _binary = args.next();
    let command = args.next();

    match command.as_deref() {
        Some("plan") => print_plan(),
        Some("probe") => run_probe_command(args.collect()),
        Some("catchup-snapshot") => run_catchup_snapshot_command(args.collect()),
        Some("catchup-progress") => run_catchup_progress_command(args.collect()),
        Some("sync-table") => sync_cli::run_sync_table_command(args.collect(), USAGE),
        Some("sync-progress") => run_sync_progress_command(args.collect()),
        Some("table-catalog") => table_catalog::run_table_catalog_command(args.collect(), USAGE),
        Some("sync-catalog") => table_catalog::run_sync_catalog_command(args.collect(), USAGE),
        Some("sync-schema") => sync_schema::run_sync_schema_command(args.collect(), USAGE),
        Some("drift-check") => run_drift_check_command(args.collect()),
        Some("repair-drift") => repair_drift::run_repair_drift_command(args.collect(), USAGE),
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

fn run_sync_progress_command(args: Vec<String>) {
    sync_progress_cli::run_sync_progress_command(args, USAGE)
}

fn run_targeted_conflict_resolution_command(args: Vec<String>) {
    targeted_conflict_resolution::run_targeted_conflict_resolution_command(args, USAGE)
}

fn exit_unknown_command(command: &str) {
    eprintln!("unknown command: {command}\n\n{USAGE}");
    std::process::exit(2);
}

fn run_resync_stream_command(mut args: Vec<String>) {
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
    let config = lost_binlog_recovery::ResyncStreamConfig {
        source: mysql_snapshot::MySqlConnectionConfig {
            host: apply.source.host,
            port: apply.source.port,
            user: apply.source.user,
            password: apply.source.password,
            database: source_database,
        },
        source_identity: apply.source_identity,
        target: apply.target,
        checkpoint_table: apply.checkpoint_table,
        progress_table: "cdc.table_sync_runs".to_string(),
        chunk_size: 10_000,
    };
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
        source: mysql_snapshot::MySqlConnectionConfig {
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
        progress_table: "cdc.table_sync_runs".to_string(),
        chunk_size: 10_000,
    }
}

fn print_recovery_report(report: &lost_binlog_recovery::RecoverLostBinlogReport) {
    println!(
        "{}",
        serde_json::to_string_pretty(report).expect("recovery report JSON")
    );
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

fn run_drift_check_command(args: Vec<String>) {
    let config = match parse_drift_check_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{USAGE}");
            std::process::exit(2);
        }
    };

    let observer = drift_check::StderrDriftCheckObserver;
    match drift_check::run_drift_check_with_observer(&config, &observer) {
        Ok(report) => {
            println!("{}", drift_check::format_drift_report(&report));
            if report.has_mismatches() {
                std::process::exit(3);
            }
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
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

fn run_catchup_snapshot_command(args: Vec<String>) {
    let config = match parse_catchup_snapshot_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{USAGE}");
            std::process::exit(2);
        }
    };

    if let Err(error) = mysql_snapshot::run_catchup_snapshot(&config) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run_catchup_progress_command(args: Vec<String>) {
    let progress_file = match parse_progress_file(args) {
        Ok(progress_file) => progress_file,
        Err(error) => {
            eprintln!("{error}\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    let store = snapshot::FileSnapshotProgressStore::new(progress_file);

    match snapshot::SnapshotProgressStore::load(&store) {
        Ok(progress) => println!("{}", snapshot::format_progress(&progress)),
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

Initial phases:
1. Snapshot source tables into target in primary-key chunks.
2. Stream MariaDB binlog from a recorded start position.
3. Apply supported row and statement events to the target.
4. Quarantine unsupported events with exact binlog coordinates.
5. Validate counts/checksums before cutover.
"
    );
}

fn parse_catchup_snapshot_config(
    args: Vec<String>,
) -> Result<mysql_snapshot::CatchupSnapshotConfig, String> {
    let mut config = mysql_snapshot::CatchupSnapshotConfig {
        source: mysql_snapshot::MySqlConnectionConfig::default(),
        target: live::TargetMySqlConfig::default(),
        progress_file: PathBuf::new(),
        progress_table: "cdc.table_sync_progress".to_string(),
        chunk_size: 10_000,
        throttle: Duration::ZERO,
        parallel_workers: 1,
        table: None,
    };
    let mut index = 0;

    while index < args.len() {
        let flag = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value"))?;

        catchup_snapshot_option(&mut config, flag, value)?;
        index += 2;
    }

    Ok(config)
}

fn parse_progress_file(args: Vec<String>) -> Result<PathBuf, String> {
    if args.len() != 2 {
        return Err("catchup-progress needs --progress-file PATH".to_string());
    }
    if args[0] != "--progress-file" {
        return Err(format!("unknown catchup-progress option: {}", args[0]));
    }

    Ok(PathBuf::from(&args[1]))
}

fn catchup_snapshot_option(
    config: &mut mysql_snapshot::CatchupSnapshotConfig,
    flag: &str,
    value: &str,
) -> Result<(), String> {
    if catchup_source_option(&mut config.source, flag, value)? {
        return Ok(());
    }
    if apply_target_option(&mut config.target, flag, value)? {
        return Ok(());
    }

    match flag {
        "--progress-file" => config.progress_file = PathBuf::from(value),
        "--progress-table" => config.progress_table = value.to_string(),
        "--chunk-size" => config.chunk_size = parse_usize(flag, value)?,
        "--throttle-ms" => config.throttle = Duration::from_millis(parse_u64(flag, value)?),
        "--parallel-workers" => config.parallel_workers = parse_usize(flag, value)?,
        "--table" => config.table = Some(value.to_string()),
        other => return Err(format!("unknown catchup-snapshot option: {other}")),
    }

    Ok(())
}

fn catchup_source_option(
    source: &mut mysql_snapshot::MySqlConnectionConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--source-host" => source.host = value.to_string(),
        "--source-port" => source.port = parse_u16(flag, value)?,
        "--source-user" => source.user = value.to_string(),
        "--source-password-env" => source.password = read_env_password(value)?,
        "--source-database" => source.database = value.to_string(),
        _ => return Ok(false),
    }

    Ok(true)
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

fn parse_drift_check_config(args: Vec<String>) -> Result<drift_check::DriftCheckConfig, String> {
    let mut config = drift_check::DriftCheckConfig {
        source: mysql_snapshot::MySqlConnectionConfig::default(),
        target: live::TargetMySqlConfig::default(),
        tables: Vec::new(),
        content_check: true,
        chunk_size: 10000,
    };
    let mut index = 0;

    while index < args.len() {
        let flag = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value"))?;

        drift_check_option(&mut config, flag, value)?;
        index += 2;
    }

    Ok(config)
}

fn drift_check_option(
    config: &mut drift_check::DriftCheckConfig,
    flag: &str,
    value: &str,
) -> Result<(), String> {
    if catchup_source_option(&mut config.source, flag, value)? {
        return Ok(());
    }
    if apply_target_option(&mut config.target, flag, value)? {
        return Ok(());
    }

    match flag {
        "--table" => config.tables.push(value.to_string()),
        "--content-check" => config.content_check = parse_bool(flag, value)?,
        "--chunk-size" => config.chunk_size = parse_usize(flag, value)?,
        other => return Err(format!("unknown drift-check option: {other}")),
    }

    Ok(())
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
        "--conflict-table" => config.conflict_table = value.to_string(),
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
        #[cfg(feature = "integration-failpoints")]
        "--integration-failpoint" => {
            config.integration_failpoint = Some(live::IntegrationFailpoint::parse(value)?);
        }
        #[cfg(feature = "integration-failpoints")]
        "--integration-logical-source-file" => {
            config.integration_logical_source_file = Some(value.to_string());
        }
        #[cfg(feature = "integration-failpoints")]
        "--integration-logical-start-position" => {
            config.integration_logical_start_position = Some(parse_u64(flag, value)?);
        }
        #[cfg(feature = "integration-failpoints")]
        "--integration-logical-checkpoint-position" => {
            config.integration_logical_checkpoint_position = Some(parse_u64(flag, value)?);
        }
        #[cfg(feature = "integration-failpoints")]
        "--integration-logical-end-position" => {
            config.integration_logical_end_position = Some(parse_u64(flag, value)?);
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
