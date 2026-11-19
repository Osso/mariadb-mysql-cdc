use super::*;
use crate::live::structured_stream::zero_query_position::ZeroQueryPosition;
use mysql_cdc::providers::mariadb::{events::gtid_event::GtidEvent, gtid::gtid::Gtid};

const BINLOG_FILE: &str = "fixture-bin.000005";

#[test]
fn zero_ended_ddl_uses_the_adjacent_mariadb_gtid_without_executing_or_checkpointing() {
    let (error, journal, target_operations) = dispatch_zero_ended_ddl(Some(561));

    assert!(matches!(error, ApplyBinlogError::DdlBlocked(_)));
    assert!(error.to_string().contains("fixture-bin.000005:427"));
    assert_eq!(journal, ["TRANSLATION_PENDING"]);
    assert!(target_operations.is_empty());
}

#[test]
fn zero_ended_ddl_without_a_trusted_anchor_does_not_write_a_journal_row() {
    let (error, journal, target_operations) = dispatch_zero_ended_ddl(None);

    assert!(matches!(error, ApplyBinlogError::DdlBlocked(_)));
    assert!(error.to_string().contains("missing physical DDL position"));
    assert!(journal.is_empty());
    assert!(target_operations.is_empty());
}

#[test]
fn zero_query_end_requires_an_immediate_physical_gtid_on_the_same_server() {
    for (gtid_header, query_header, between, expected) in [
        (wire_header(427, 42), wire_header(0, 134), false, Some(561)),
        (wire_header(427, 42), wire_header(0, 134), true, None),
        (wire_header(0, 42), wire_header(0, 134), false, None),
        (wire_header(41, 42), wire_header(0, 134), false, None),
        (wire_header(u32::MAX, 42), wire_header(0, 134), false, None),
        (wire_header(427, 42), wire_header(0, 18), false, None),
        (
            EventHeader {
                event_flags: 0x20,
                ..wire_header(427, 42)
            },
            wire_header(0, 134),
            false,
            None,
        ),
        (
            wire_header(427, 42),
            EventHeader {
                server_id: 4,
                ..wire_header(0, 134)
            },
            false,
            None,
        ),
        (wire_header(427, 42), wire_header(562, 134), false, None),
    ] {
        let mut position = ZeroQueryPosition::default();
        position.observe(&gtid_header, &maria_gtid());
        if between {
            let rotate = BinlogEvent::RotateEvent(RotateEvent {
                binlog_filename: BINLOG_FILE.into(),
                binlog_position: 4,
            });
            position.observe(&wire_header(0, 47), &rotate);
        }
        assert_eq!(position.observe(&query_header, &zero_ended_ddl()), expected);
        assert_eq!(position.observe(&query_header, &zero_ended_ddl()), None);
    }
}

fn dispatch_zero_ended_ddl(
    recovered_query_end: Option<u32>,
) -> (ApplyBinlogError, Vec<&'static str>, Vec<&'static str>) {
    let mut applier = RowApplier::new(TransactionRecordingExecutor::default());
    let journal = RecordingDdlReplayJournal::default();
    let mut state = StructuredEventState::new(Some("fixture_cdc".into()));
    state.record_event_position(427, recovered_query_end);
    let mut file = BINLOG_FILE.to_string();
    let mut context = StreamEventContext {
        schema_resolver: &FixtureSchemaResolver,
        state: &mut state,
        target_transaction: &mut TargetTransaction::default(),
        checkpoint_store: Some(&NoopCheckpointStore),
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file: &mut file,
        group_config: TargetTransactionGroupConfig::default(),
    };
    let error = handle_ddl_event(
        &mut applier,
        &journal,
        &RecordingSemanticInventory::default(),
        "synthetic-source",
        &mut context,
        &wire_header(0, 134),
        &zero_ended_ddl(),
    )
    .expect_err("zero-ended DDL must not enter automatic replay");
    let journal_operations = journal.operations.borrow().to_vec();
    let target_operations = applier.executor().operations();
    (error, journal_operations, target_operations)
}

fn wire_header(end: u32, length: u32) -> EventHeader {
    EventHeader {
        timestamp: 100,
        event_type: 2,
        server_id: 3,
        event_length: length,
        next_event_position: end,
        event_flags: 0,
    }
}

fn maria_gtid() -> BinlogEvent {
    BinlogEvent::MariaDbGtidEvent(GtidEvent {
        gtid: Gtid::new(1, 3, 9),
        flags: 0,
    })
}

fn zero_ended_ddl() -> BinlogEvent {
    BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".into(),
        sql_statement: "CREATE TABLE IF NOT EXISTS old_stock (id INT NOT NULL)".into(),
    })
}
