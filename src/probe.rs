use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, SslOpts};
use std::collections::BTreeMap;

const DEFAULT_HOST: &str = "127.0.0.1";

#[derive(Clone, Debug)]
pub struct ProbeConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub binlog_file: Option<String>,
    pub start_position: Option<u64>,
    pub stop_position: Option<u64>,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_string(),
            port: 3306,
            user: String::new(),
            password: String::new(),
            binlog_file: None,
            start_position: None,
            stop_position: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinlogCoordinate {
    pub file: String,
    pub position: u64,
}

#[derive(Clone, Debug)]
pub struct ProbeReport {
    pub source_host: String,
    pub source_port: u16,
    pub source_user: String,
    pub start_coordinate: BinlogCoordinate,
    pub stop_coordinate: Option<BinlogCoordinate>,
    pub executed_gtid_set: Option<String>,
    pub events: Vec<ClassifiedEvent>,
    pub event_totals: BTreeMap<EventClass, usize>,
}

#[derive(Clone, Debug)]
pub struct ClassifiedEvent {
    pub coordinate: BinlogCoordinate,
    pub class: EventClass,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EventClass {
    Rotate,
    Gtid,
    TableMap,
    RowsInsert,
    RowsUpdate,
    RowsDelete,
    Statement,
    Query,
    Unknown,
}

#[derive(Debug)]
pub struct ProbeError {
    message: String,
}

impl std::fmt::Display for ProbeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ProbeError {}

impl ProbeError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub struct ProcessRunner;

pub trait ProbeProcessRunner {
    fn show_master_status(&self, config: &ProbeConfig) -> Result<MasterStatus, ProbeError>;
    fn read_binlog(
        &self,
        config: &ProbeConfig,
        start: &BinlogCoordinate,
        stop_position: Option<u64>,
    ) -> Result<String, ProbeError>;
}

pub fn current_master_coordinate(config: &ProbeConfig) -> Result<BinlogCoordinate, ProbeError> {
    let runner = ProcessRunner;
    let status = runner.show_master_status(config)?;
    Ok(BinlogCoordinate {
        file: status.binlog_file,
        position: status.position,
    })
}

pub fn run_probe(
    config: &ProbeConfig,
    runner: &mut impl ProbeProcessRunner,
) -> Result<ProbeReport, ProbeError> {
    let status = runner.show_master_status(config)?;

    let start_coordinate = BinlogCoordinate {
        file: config
            .binlog_file
            .clone()
            .unwrap_or_else(|| status.binlog_file.clone()),
        position: config.start_position.unwrap_or(status.position),
    };

    let output = runner.read_binlog(config, &start_coordinate, config.stop_position)?;
    let (events, event_totals) = classify_binlog_events(&output, &start_coordinate);

    let stop_coordinate = config.stop_position.map(|position| BinlogCoordinate {
        file: if let Some(rotate) = events
            .iter()
            .rev()
            .find_map(|event| event.class.is_rotate().then_some(&event.coordinate.file))
        {
            rotate.clone()
        } else {
            start_coordinate.file.clone()
        },
        position,
    });

    Ok(ProbeReport {
        source_host: config.host.clone(),
        source_port: config.port,
        source_user: config.user.clone(),
        start_coordinate,
        stop_coordinate,
        executed_gtid_set: status.executed_gtid_set,
        events,
        event_totals,
    })
}

impl EventClass {
    fn display_name(&self) -> &'static str {
        match self {
            Self::Rotate => "Rotate",
            Self::Gtid => "Gtid",
            Self::TableMap => "TableMap",
            Self::RowsInsert => "RowsInsert",
            Self::RowsUpdate => "RowsUpdate",
            Self::RowsDelete => "RowsDelete",
            Self::Statement => "Statement",
            Self::Query => "Query",
            Self::Unknown => "Unknown",
        }
    }

    fn is_rotate(&self) -> bool {
        matches!(self, Self::Rotate)
    }
}

fn classify_binlog_events(
    output: &str,
    start: &BinlogCoordinate,
) -> (Vec<ClassifiedEvent>, BTreeMap<EventClass, usize>) {
    let mut current_file = start.file.clone();
    let mut current_position = start.position;
    let mut events = Vec::new();
    let mut totals = BTreeMap::new();

    for line in output.lines() {
        let line = line.trim_end();

        if let Some(position) = parse_at_position(line) {
            current_position = position;
            continue;
        }

        if let Some(rotate_file) = parse_rotate_file(line) {
            current_file = rotate_file;
            let event = ClassifiedEvent {
                coordinate: current_coordinate(&current_file, current_position),
                class: EventClass::Rotate,
                detail: line.to_string(),
            };
            record_event(event, &mut events, &mut totals);
            continue;
        }

        if let Some(class) = classify_line(line) {
            if class == EventClass::Unknown && line.trim_start().starts_with("### @") {
                continue;
            }

            let event = ClassifiedEvent {
                coordinate: current_coordinate(&current_file, current_position),
                class,
                detail: line.to_string(),
            };
            record_event(event, &mut events, &mut totals);
        }
    }

    (events, totals)
}

#[cfg(test)]
fn parse_master_status(output: &str) -> Result<MasterStatus, ProbeError> {
    let mut lines = output.lines().filter(|line| !line.trim().is_empty());
    let line = lines
        .next()
        .ok_or_else(|| ProbeError::new("SHOW MASTER STATUS returned no rows"))?;

    let fields: Vec<&str> = line.split('\t').map(|field| field.trim()).collect();
    let file = fields
        .first()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ProbeError::new("SHOW MASTER STATUS missing binlog file"))?
        .to_string();

    let position = fields
        .get(1)
        .ok_or_else(|| ProbeError::new("SHOW MASTER STATUS missing position"))?
        .parse::<u64>()
        .map_err(|_| ProbeError::new("SHOW MASTER STATUS position is not numeric"))?;

    // Executed_Gtid_Set is typically column 5.
    let executed_gtid_set = fields
        .get(4)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());

    Ok(MasterStatus {
        binlog_file: file,
        position,
        executed_gtid_set,
    })
}

fn parse_at_position(line: &str) -> Option<u64> {
    let line = line.trim();
    if !line.starts_with("# at ") {
        return None;
    }
    let rest = line.strip_prefix("# at ")?;
    rest.split_whitespace()
        .next()
        .and_then(|value| value.parse::<u64>().ok())
}

fn parse_rotate_file(line: &str) -> Option<String> {
    if !line.contains("Rotate to") {
        return None;
    }

    let tail = line.split_once("Rotate to ")?.1;
    let name = tail
        .split(|ch: char| ch.is_whitespace() || ch == ',')
        .next()
        .unwrap_or_default()
        .trim_matches(|ch: char| ch == '"' || ch == '\'' || ch == '`');

    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

fn classify_line(line: &str) -> Option<EventClass> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }

    if line.starts_with('#') && !line.starts_with("###") {
        return classify_comment_line(line);
    }

    let upper = line.to_ascii_uppercase();

    if upper.starts_with("### INSERT INTO") {
        return Some(EventClass::RowsInsert);
    }
    if upper.starts_with("### UPDATE") {
        return Some(EventClass::RowsUpdate);
    }
    if upper.starts_with("### DELETE") {
        return Some(EventClass::RowsDelete);
    }

    if upper.starts_with("INSERT INTO")
        || upper.starts_with("UPDATE ")
        || upper.starts_with("DELETE FROM")
        || upper.starts_with("CREATE ")
        || upper.starts_with("ALTER ")
        || upper.starts_with("DROP ")
        || upper.starts_with("TRUNCATE ")
    {
        return Some(EventClass::Statement);
    }

    Some(EventClass::Unknown)
}

fn classify_comment_line(line: &str) -> Option<EventClass> {
    if line.contains("Rotate to") {
        return Some(EventClass::Rotate);
    }
    if line.to_ascii_uppercase().starts_with("# QUERY") {
        return Some(EventClass::Query);
    }
    if line.contains("GTID") {
        return Some(EventClass::Gtid);
    }
    if line.contains("Table_map:") {
        return Some(EventClass::TableMap);
    }

    None
}

fn record_event(
    event: ClassifiedEvent,
    events: &mut Vec<ClassifiedEvent>,
    totals: &mut BTreeMap<EventClass, usize>,
) {
    *totals.entry(event.class.clone()).or_insert(0) += 1;
    events.push(event);
}

fn current_coordinate(file: &str, position: u64) -> BinlogCoordinate {
    BinlogCoordinate {
        file: file.to_string(),
        position,
    }
}

#[derive(Clone, Debug)]
pub struct MasterStatus {
    binlog_file: String,
    position: u64,
    executed_gtid_set: Option<String>,
}

impl ProbeProcessRunner for ProcessRunner {
    fn show_master_status(&self, config: &ProbeConfig) -> Result<MasterStatus, ProbeError> {
        let mut conn = open_probe_connection(config)?;
        conn.query_first::<(String, u64, Option<String>, Option<String>, Option<String>), _>(
            "SHOW MASTER STATUS",
        )
        .map_err(probe_mysql_error)?
        .map(master_status_from_row)
        .ok_or_else(|| ProbeError::new("SHOW MASTER STATUS returned no rows"))
    }

    fn read_binlog(
        &self,
        _config: &ProbeConfig,
        _start: &BinlogCoordinate,
        _stop_position: Option<u64>,
    ) -> Result<String, ProbeError> {
        Err(ProbeError::new(
            "probe binlog text mode was removed; use stream-binlog native replication",
        ))
    }
}

fn open_probe_connection(config: &ProbeConfig) -> Result<Conn, ProbeError> {
    Conn::new(probe_opts(config)).map_err(probe_mysql_error)
}

fn probe_opts(config: &ProbeConfig) -> Opts {
    let builder = OptsBuilder::default()
        .ip_or_hostname(Some(&config.host))
        .tcp_port(config.port)
        .user(Some(&config.user))
        .pass(Some(&config.password))
        .prefer_socket(false)
        .ssl_opts(
            SslOpts::default()
                .with_danger_skip_domain_validation(true)
                .with_danger_accept_invalid_certs(true),
        );
    Opts::from(builder)
}

fn probe_mysql_error(error: mysql::Error) -> ProbeError {
    ProbeError::new(error.to_string())
}

fn master_status_from_row(
    row: (String, u64, Option<String>, Option<String>, Option<String>),
) -> MasterStatus {
    MasterStatus {
        binlog_file: row.0,
        position: row.1,
        executed_gtid_set: row.4.filter(|value| !value.trim().is_empty()),
    }
}

#[cfg(test)]
fn build_binlog_args(
    config: &ProbeConfig,
    start: &BinlogCoordinate,
    stop_position: Option<u64>,
) -> Vec<String> {
    let mut args = vec![
        "--read-from-remote-server".to_string(),
        "--verbose".to_string(),
        "--base64-output=decode-rows".to_string(),
        "--host".to_string(),
        config.host.clone(),
        "--port".to_string(),
        config.port.to_string(),
        "--user".to_string(),
        config.user.clone(),
        "--start-position".to_string(),
        start.position.to_string(),
    ];

    if let Some(stop_position) = stop_position {
        args.push("--stop-position".to_string());
        args.push(stop_position.to_string());
    }

    args.push(start.file.clone());
    args
}

pub fn print_report(report: &ProbeReport) {
    println!(
        "Connected to {}:{} as {}",
        report.source_host, report.source_port, report.source_user
    );
    println!(
        "Start coordinate: {}:{}",
        report.start_coordinate.file, report.start_coordinate.position
    );
    if let Some(stop_coordinate) = &report.stop_coordinate {
        println!(
            "Stop coordinate: {}:{}",
            stop_coordinate.file, stop_coordinate.position
        );
    }
    if let Some(executed_gtid_set) = &report.executed_gtid_set {
        println!("Executed_Gtid_Set: {executed_gtid_set}");
    }

    println!("Event totals:");
    for (class, count) in &report.event_totals {
        println!("  {}: {count}", class.display_name());
    }

    println!("Classified events ({}):", report.events.len());
    for event in &report.events {
        println!(
            "  {}:{} {} {}",
            event.coordinate.file,
            event.coordinate.position,
            event.class.display_name(),
            event.detail,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BinlogCoordinate, EventClass, ProbeConfig, ProbeProcessRunner, ProbeReport,
        build_binlog_args, classify_binlog_events, classify_line, parse_at_position,
        parse_master_status, parse_rotate_file, run_probe,
    };

    struct FakeProcessRunner {
        status: String,
        events: String,
    }

    impl ProbeProcessRunner for FakeProcessRunner {
        fn show_master_status(
            &self,
            _config: &ProbeConfig,
        ) -> Result<super::MasterStatus, super::ProbeError> {
            super::parse_master_status(&self.status)
        }

        fn read_binlog(
            &self,
            _config: &ProbeConfig,
            _start: &BinlogCoordinate,
            _stop_position: Option<u64>,
        ) -> Result<String, super::ProbeError> {
            Ok(self.events.clone())
        }
    }

    #[test]
    fn parse_master_status_parses_file_position_and_gtid_set() {
        let parsed = parse_master_status("mysql-bin.000010\t5678\trepl_db\t\t0-1-1,0-1-2")
            .expect("status parse");

        assert_eq!(parsed.binlog_file, "mysql-bin.000010");
        assert_eq!(parsed.position, 5678);
        assert_eq!(parsed.executed_gtid_set, Some("0-1-1,0-1-2".to_string()));
    }

    #[test]
    fn parse_master_status_reports_missing_or_bad_fields() {
        assert_eq!(
            parse_master_status("")
                .expect_err("empty status")
                .to_string(),
            "SHOW MASTER STATUS returned no rows"
        );
        assert_eq!(
            parse_master_status("\t123")
                .expect_err("missing file")
                .to_string(),
            "SHOW MASTER STATUS missing binlog file"
        );
        assert_eq!(
            parse_master_status("mysql-bin.000010")
                .expect_err("missing position")
                .to_string(),
            "SHOW MASTER STATUS missing position"
        );
        assert_eq!(
            parse_master_status("mysql-bin.000010\tnot-a-number")
                .expect_err("bad position")
                .to_string(),
            "SHOW MASTER STATUS position is not numeric"
        );
    }

    #[test]
    fn parse_at_position_detects_file_offsets() {
        assert_eq!(parse_at_position("# at 123"), Some(123));
        assert_eq!(parse_at_position("#  at 123"), None);
    }

    #[test]
    fn parses_rotate_file_from_verbose_binlog_line() {
        assert_eq!(
            parse_rotate_file("# Rotate to `mysql-bin.000011`, pos: 4"),
            Some("mysql-bin.000011".to_string())
        );
        assert_eq!(parse_rotate_file("# Query: not a rotate event"), None);
    }

    #[test]
    fn classifies_statement_row_and_unknown_lines() {
        assert_eq!(classify_line(""), None);
        assert_eq!(
            classify_line("# Table_map: `app`.`users`"),
            Some(EventClass::TableMap)
        );
        assert_eq!(classify_line("# GTID 0-1-2"), Some(EventClass::Gtid));
        assert_eq!(classify_line("# Query"), Some(EventClass::Query));
        assert_eq!(
            classify_line("### UPDATE `app`.`users`"),
            Some(EventClass::RowsUpdate)
        );
        assert_eq!(
            classify_line("### DELETE FROM `app`.`users`"),
            Some(EventClass::RowsDelete)
        );
        assert_eq!(
            classify_line("ALTER TABLE users ADD name varchar(255)"),
            Some(EventClass::Statement)
        );
        assert_eq!(
            classify_line("unrecognized payload"),
            Some(EventClass::Unknown)
        );
    }

    #[test]
    fn builds_remote_binlog_args_with_stop_position_before_file() {
        let config = ProbeConfig {
            host: "10.0.0.2".to_string(),
            port: 3307,
            user: "cdc".to_string(),
            password: "secret".to_string(),
            ..ProbeConfig::default()
        };

        let args = build_binlog_args(
            &config,
            &BinlogCoordinate {
                file: "mysqld-bin.000777".to_string(),
                position: 12345,
            },
            Some(45678),
        );

        assert_eq!(
            args,
            vec![
                "--read-from-remote-server",
                "--verbose",
                "--base64-output=decode-rows",
                "--host",
                "10.0.0.2",
                "--port",
                "3307",
                "--user",
                "cdc",
                "--start-position",
                "12345",
                "--stop-position",
                "45678",
                "mysqld-bin.000777",
            ]
        );
    }

    #[test]
    fn run_probe_produces_classified_events() {
        let config = ProbeConfig {
            user: "binlog_reader".to_string(),
            password: "secret".to_string(),
            ..ProbeConfig::default()
        };

        let mut runner = FakeProcessRunner {
            status: "mysql-bin.000010\t4\t\t\t".to_string(),
            events: [
                "# at 4",
                concat!(
                    "#250601 12:00:00 server id 1  end_log_pos 120  CRC32 ",
                    "0x",
                    "00000000"
                ),
                "# Query: rotating",
                "#  at 120",
                "# Rotate to mysql-bin.000011",
                "# at 4",
                "# Query: 9f5a...",
                "### INSERT INTO `app`.`users`",
                "### @1=1",
                "CREATE TABLE users (id INT)",
            ]
            .join("\n"),
        };

        let report: ProbeReport = run_probe(&config, &mut runner).expect("probe");

        assert_eq!(report.start_coordinate.file, "mysql-bin.000010");
        assert_eq!(report.start_coordinate.position, 4);
        assert_eq!(
            report.event_totals.get(&super::EventClass::Rotate),
            Some(&1)
        );
        assert_eq!(
            report.event_totals.get(&super::EventClass::RowsInsert),
            Some(&1)
        );
        assert_eq!(
            report.event_totals.get(&super::EventClass::Statement),
            Some(&1)
        );
    }

    #[test]
    fn classify_event_lines() {
        let (events, totals) = classify_binlog_events(
            "# at 4\n### INSERT INTO `a`.`b`\nUPDATE something\n### DELETE FROM `a`.`b`\n",
            &BinlogCoordinate {
                file: "mysql-bin.000001".to_string(),
                position: 4,
            },
        );
        assert_eq!(events[0].class, super::EventClass::RowsInsert);
        assert_eq!(events[1].class, super::EventClass::Statement);
        assert_eq!(events[2].class, super::EventClass::RowsDelete);
        assert_eq!(totals.get(&super::EventClass::RowsInsert), Some(&1));
        assert_eq!(totals.get(&super::EventClass::Statement), Some(&1));
        assert_eq!(totals.get(&super::EventClass::RowsDelete), Some(&1));
    }

    #[test]
    fn binlog_command_uses_text_output_and_file_last() {
        let config = ProbeConfig {
            stop_position: Some(200),
            ..ProbeConfig::default()
        };
        let start = BinlogCoordinate {
            file: "mysql-bin.000001".to_string(),
            position: 100,
        };

        let args = super::build_binlog_args(&config, &start, config.stop_position);

        assert!(!args.contains(&"--raw".to_string()));
        assert_eq!(args.last(), Some(&"mysql-bin.000001".to_string()));
        assert!(args.contains(&"--stop-position".to_string()));
        assert!(args.contains(&"200".to_string()));
    }
}
