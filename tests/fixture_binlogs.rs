use mysql_cdc::binlog_reader::BinlogReader;
use mysql_cdc::events::binlog_event::BinlogEvent;
use std::collections::BTreeSet;
use std::fs::File;

#[test]
fn mixed_binlog_fixture_covers_required_event_types() {
    let mut seen = FixtureCoverage::default();

    read_fixture("fixtures/mixed-binlog/mysql-bin.000001", &mut seen);
    read_fixture("fixtures/mixed-binlog/mysql-bin.000002", &mut seen);

    assert!(
        seen.errors.is_empty(),
        "fixture parse errors: {:#?}",
        seen.errors
    );
    assert!(seen.event_types.contains("MariaDbGtidEvent"));
    assert!(seen.event_types.contains("QueryEvent"));
    assert!(seen.event_types.contains("TableMapEvent"));
    assert!(seen.event_types.contains("WriteRowsEvent"));
    assert!(seen.event_types.contains("UpdateRowsEvent"));
    assert!(seen.event_types.contains("DeleteRowsEvent"));
    assert!(seen.event_types.contains("RotateEvent"));
    assert!(
        seen.query_sql
            .iter()
            .any(|sql| sql.contains("CREATE DATABASE fixture_cdc"))
    );
    assert!(
        seen.query_sql
            .iter()
            .any(|sql| sql.contains("CREATE TABLE accounts"))
    );
    assert!(
        seen.query_sql
            .iter()
            .any(|sql| sql.contains("ALTER TABLE accounts ADD COLUMN status"))
    );
}

#[derive(Default)]
struct FixtureCoverage {
    errors: Vec<String>,
    event_types: BTreeSet<&'static str>,
    query_sql: Vec<String>,
}

fn read_fixture(path: &str, seen: &mut FixtureCoverage) {
    let file = File::open(path).expect("open fixture");
    let reader = BinlogReader::new(file).expect("create binlog reader");

    for event in reader.read_events() {
        match event {
            Ok((_header, event)) => record_event(event, seen),
            Err(error) => seen.errors.push(format!("{path}: {error:?}")),
        }
    }
}

fn record_event(event: BinlogEvent, seen: &mut FixtureCoverage) {
    seen.event_types.insert(event_name(&event));
    if let BinlogEvent::QueryEvent(query) = event {
        seen.query_sql.push(query.sql_statement);
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
