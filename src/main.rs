pub mod checkpoint;
pub mod cutover;
pub mod inventory;
pub mod live;
mod probe;
pub mod rehearsal;
pub mod row;
pub mod snapshot;
pub mod statement;
pub mod target;
pub mod validation;

use std::env;

const USAGE: &str = "\
mariadb-mysql-cdc

Usage:
  mariadb-mysql-cdc plan
  mariadb-mysql-cdc probe --host HOST --user USER --password-env ENV [options]
  mariadb-mysql-cdc apply-binlog --source-host HOST --source-user USER --source-password-env ENV --target-host HOST --target-user USER --target-password-env ENV --target-database DB [options]

Commands:
  plan    Print the current migration tool design.
  probe   Read source binlog coordinates and classify MariaDB binlog events.
  apply-binlog
          Read remote MariaDB binlog text and apply compatible statements.

Probe options:
  --host HOST                 MariaDB source host.
  --port PORT                 MariaDB source port. Defaults to 3306.
  --user USER                 MariaDB replication user.
  --password-env ENV          Environment variable containing the password.
  --binlog-file FILE          Override SHOW MASTER STATUS binlog file.
  --start-position POSITION   Override SHOW MASTER STATUS position.
  --stop-position POSITION    Stop reading at a binlog position.
  --mariadb PATH              mariadb client path. Defaults to mariadb.
  --mariadb-binlog PATH       mariadb-binlog path. Defaults to mariadb-binlog.

Apply options:
  --source-host HOST              MariaDB source host.
  --source-port PORT              MariaDB source port. Defaults to 3306.
  --source-user USER              MariaDB replication user.
  --source-password-env ENV       Environment variable containing source password.
  --binlog-file FILE              Source binlog file.
  --start-position POSITION       Source binlog start position.
  --stop-position POSITION        Stop reading at source binlog position.
  --target-host HOST              MySQL target host.
  --target-port PORT              MySQL target port. Defaults to 3306.
  --target-user USER              MySQL target user.
  --target-password-env ENV       Environment variable containing target password.
  --target-database DB            MySQL target database.
  --mariadb PATH                  mariadb client path. Defaults to mariadb.
  --mariadb-binlog PATH           mariadb-binlog path. Defaults to mariadb-binlog.
";

fn main() {
    let mut args = env::args();
    let _binary = args.next();
    let command = args.next();

    match command.as_deref() {
        Some("plan") => print_plan(),
        Some("probe") => run_probe_command(args.collect()),
        Some("apply-binlog") => run_apply_binlog_command(args.collect()),
        Some("-h" | "--help") | None => print!("{USAGE}"),
        Some(other) => {
            eprintln!("unknown command: {other}\n\n{USAGE}");
            std::process::exit(2);
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

fn print_plan() {
    println!(
        "\
Goal: migrate MariaDB to MySQL-compatible targets with minimal downtime.

Constraints:
- Keep production MariaDB binlog_format=MIXED.
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
            "--mariadb" => config.mariadb = value.clone(),
            "--mariadb-binlog" => config.mariadb_binlog = value.clone(),
            other => return Err(format!("unknown probe option: {other}")),
        }

        index += 2;
    }

    Ok(config)
}

fn parse_apply_binlog_config(args: Vec<String>) -> Result<live::ApplyBinlogConfig, String> {
    let mut config = live::ApplyBinlogConfig::default();
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

    match flag {
        "--mariadb" => config.mariadb = value.to_string(),
        "--mariadb-binlog" => config.mariadb_binlog = value.to_string(),
        other => return Err(format!("unknown apply-binlog option: {other}")),
    }

    Ok(())
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
        _ => return Ok(false),
    }

    Ok(true)
}

fn read_env_password(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("{name} is not set"))
}

fn parse_u16(flag: &str, value: &str) -> Result<u16, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} must be an integer"))
}

fn parse_u64(flag: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} must be an integer"))
}
