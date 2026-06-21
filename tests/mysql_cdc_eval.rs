use mysql_cdc::binlog_reader::BinlogReader;
use mysql_cdc::events::binlog_event::BinlogEvent;
use std::collections::BTreeSet;
use std::fs::File;

#[test]
fn mysql_cdc_parses_mariadb_mixed_fixture_without_errors() {
    let mut report = EvaluationReport::default();

    read_fixture("fixtures/mixed-binlog/mysql-bin.000001", &mut report);
    read_fixture("fixtures/mixed-binlog/mysql-bin.000002", &mut report);

    assert!(
        report.errors.is_empty(),
        "mysql_cdc failed against fixture events: {:#?}",
        report.errors
    );
    assert_eq!(report.unknown_event_types, BTreeSet::from([161]));
    assert!(report.event_names.contains("MariaDbGtidEvent"));
    assert!(report.event_names.contains("QueryEvent"));
    assert!(report.event_names.contains("TableMapEvent"));
    assert!(report.event_names.contains("WriteRowsEvent"));
    assert!(report.event_names.contains("UpdateRowsEvent"));
    assert!(report.event_names.contains("DeleteRowsEvent"));
    assert!(report.event_names.contains("RotateEvent"));
}

#[derive(Default)]
struct EvaluationReport {
    errors: Vec<String>,
    event_names: BTreeSet<&'static str>,
    unknown_event_types: BTreeSet<u8>,
}

fn read_fixture(path: &str, report: &mut EvaluationReport) {
    let file = File::open(path).expect("open fixture");
    let reader = BinlogReader::new(file).expect("create binlog reader");

    for event in reader.read_events() {
        match event {
            Ok((header, event)) => {
                if matches!(event, BinlogEvent::UnknownEvent) {
                    report.unknown_event_types.insert(header.event_type);
                }
                report.event_names.insert(event_name(&event));
            }
            Err(error) => {
                report.errors.push(format!("{path}: {error:?}"));
            }
        }
    }
}

fn event_name(event: &BinlogEvent) -> &'static str {
    match event {
        BinlogEvent::UnknownEvent => "UnknownEvent",
        BinlogEvent::DeleteRowsEvent(_) => "DeleteRowsEvent",
        BinlogEvent::UpdateRowsEvent(_) => "UpdateRowsEvent",
        BinlogEvent::WriteRowsEvent(_) => "WriteRowsEvent",
        BinlogEvent::XidEvent(_) => "XidEvent",
        BinlogEvent::IntVarEvent(_) => "IntVarEvent",
        BinlogEvent::UserVarEvent(_) => "UserVarEvent",
        BinlogEvent::QueryEvent(_) => "QueryEvent",
        BinlogEvent::TableMapEvent(_) => "TableMapEvent",
        BinlogEvent::RotateEvent(_) => "RotateEvent",
        BinlogEvent::RowsQueryEvent(_) => "RowsQueryEvent",
        BinlogEvent::HeartbeatEvent(_) => "HeartbeatEvent",
        BinlogEvent::FormatDescriptionEvent(_) => "FormatDescriptionEvent",
        BinlogEvent::MySqlGtidEvent(_) => "MySqlGtidEvent",
        BinlogEvent::MySqlPrevGtidsEvent(_) => "MySqlPrevGtidsEvent",
        BinlogEvent::MariaDbGtidEvent(_) => "MariaDbGtidEvent",
        BinlogEvent::MariaDbGtidListEvent(_) => "MariaDbGtidListEvent",
    }
}
