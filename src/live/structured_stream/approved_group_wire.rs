use super::super::{ApplyBinlogError, ddl_event::DdlEvent};
use mysql_cdc::errors::Error as MysqlCdcError;
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::events::event_header::EventHeader;

const HEADER_BYTES: u32 = 19;
const STATEMENT_END: u16 = 1;
const MAX_WRITE_EVENTS: u16 = 256;

type SourceEvent = Result<(EventHeader, BinlogEvent), MysqlCdcError>;

pub(super) struct ApprovedWireBounds<'a> {
    pub(super) table: &'a str,
    pub(super) write_events: u16,
    pub(super) annotation_bytes: u32,
    pub(super) xid_end: u32,
}

pub(super) fn validate_source_only_group(
    events: &mut impl Iterator<Item = SourceEvent>,
    ddl: &DdlEvent,
    bounds: ApprovedWireBounds<'_>,
) -> Result<(), ApplyBinlogError> {
    if bounds.annotation_bytes < HEADER_BYTES
        || !(1..=MAX_WRITE_EVENTS).contains(&bounds.write_events)
    {
        return Err(rejected("invalid approved group bounds"));
    }
    // Dump flags omit the single annotation independently measured in the source file.
    let start = ddl
        .event_end_position
        .checked_add(u64::from(bounds.annotation_bytes))
        .ok_or_else(|| rejected("source group position overflow"))?;
    let (table_id, mut end) = read_approved_table_map(events, ddl, &bounds, start)?;
    for index in 1..=bounds.write_events {
        let (header, event) = next_event(events)?;
        end = validate_header(&header, ddl, end, bounds.xid_end)?;
        validate_write(event, table_id, index == bounds.write_events)?;
    }
    let (header, event) = next_event(events)?;
    let end = validate_header(&header, ddl, end, bounds.xid_end)?;
    if !matches!(event, BinlogEvent::XidEvent(_))
        || header.next_event_position != bounds.xid_end
        || end != u64::from(bounds.xid_end)
    {
        return Err(rejected("expected the approved XID event"));
    }
    Ok(())
}

fn read_approved_table_map(
    events: &mut impl Iterator<Item = SourceEvent>,
    ddl: &DdlEvent,
    bounds: &ApprovedWireBounds<'_>,
    start: u64,
) -> Result<(u64, u64), ApplyBinlogError> {
    let (header, event) = next_event(events)?;
    let end = validate_header(&header, ddl, start, bounds.xid_end)?;
    let BinlogEvent::TableMapEvent(table) = event else {
        return Err(rejected("expected one table map"));
    };
    if table.database_name != ddl.schema_name || table.table_name != bounds.table {
        return Err(rejected(
            "table map does not match the approved schema and table",
        ));
    }
    Ok((table.table_id, end))
}

fn validate_write(event: BinlogEvent, table_id: u64, last: bool) -> Result<(), ApplyBinlogError> {
    let BinlogEvent::WriteRowsEvent(write) = event else {
        return Err(rejected("expected an insert rows event"));
    };
    let expected_flags = if last { STATEMENT_END } else { 0 };
    if write.table_id != table_id || write.flags != expected_flags || write.rows.is_empty() {
        return Err(rejected(
            "insert rows event is unsafe or does not match the table map",
        ));
    }
    Ok(())
}

fn next_event(
    events: &mut impl Iterator<Item = SourceEvent>,
) -> Result<(EventHeader, BinlogEvent), ApplyBinlogError> {
    events
        .next()
        .ok_or_else(|| rejected("source group ended before its approved XID"))?
        .map_err(|_| rejected("source event could not be read"))
}

fn validate_header(
    header: &EventHeader,
    ddl: &DdlEvent,
    start: u64,
    xid_end: u32,
) -> Result<u64, ApplyBinlogError> {
    let end = start
        .checked_add(u64::from(header.event_length))
        .filter(|end| *end <= u64::from(xid_end))
        .ok_or_else(|| rejected("source event crosses the approved XID"))?;
    let matches_position =
        header.next_event_position == 0 || u64::from(header.next_event_position) == end;
    if header.server_id != ddl.source_server_id
        || header.timestamp == 0
        || header.event_flags != 0
        || header.event_length < HEADER_BYTES
        || !matches_position
    {
        return Err(rejected(
            "source event identity, flags, or position is unsafe",
        ));
    }
    Ok(end)
}

fn rejected(reason: &str) -> ApplyBinlogError {
    ApplyBinlogError::Statement(format!("approved source-only group rejected: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_cdc::events::row_events::row_data::RowData;
    use mysql_cdc::events::row_events::write_rows_event::WriteRowsEvent;
    use mysql_cdc::events::table_map_event::TableMapEvent;
    use mysql_cdc::events::xid_event::XidEvent;

    const SERVER_ID: u32 = 41;
    const DDL_END: u64 = 100;
    const XID_END: u32 = 185;
    const ANNOTATION_BYTES: u32 = 25;
    const TABLE_ID: u64 = 9;

    fn ddl() -> DdlEvent {
        DdlEvent {
            source_identity: "source".into(),
            source_server_id: SERVER_ID,
            binlog_file: "bin.000001".into(),
            event_start_position: 80,
            event_end_position: DDL_END,
            schema_name: "app".into(),
            raw_sql: "CREATE TABLE app.accounts (id INT)".into(),
        }
    }

    fn header(server_id: u32, end: u32) -> EventHeader {
        EventHeader {
            timestamp: 1,
            event_type: 0,
            server_id,
            event_length: 20,
            next_event_position: end,
            event_flags: 0,
        }
    }

    fn table_map(schema: &str, table: &str) -> BinlogEvent {
        BinlogEvent::TableMapEvent(TableMapEvent {
            table_id: TABLE_ID,
            database_name: schema.into(),
            table_name: table.into(),
            column_types: vec![3],
            column_metadata: vec![0],
            null_bitmap: vec![false],
            table_metadata: None,
        })
    }

    fn write(table_id: u64, rows: Vec<RowData>) -> BinlogEvent {
        BinlogEvent::WriteRowsEvent(WriteRowsEvent {
            table_id,
            flags: STATEMENT_END,
            columns_number: 1,
            columns_present: vec![true],
            rows,
        })
    }

    fn row() -> RowData {
        RowData::new(vec![Some(
            mysql_cdc::events::row_events::mysql_value::MySqlValue::Int(1),
        )])
    }

    fn xid() -> BinlogEvent {
        BinlogEvent::XidEvent(XidEvent { xid: 1 })
    }

    fn valid_events() -> Vec<Result<(EventHeader, BinlogEvent), MysqlCdcError>> {
        vec![
            Ok((header(SERVER_ID, 0), table_map("app", "accounts"))),
            Ok((header(SERVER_ID, 0), write(TABLE_ID, vec![row()]))),
            Ok((header(SERVER_ID, XID_END), xid())),
        ]
    }

    fn validate(
        events: &mut impl Iterator<Item = Result<(EventHeader, BinlogEvent), MysqlCdcError>>,
        expected_writes: u16,
        xid_end: u32,
    ) -> Result<(), ApplyBinlogError> {
        validate_source_only_group(
            events,
            &ddl(),
            ApprovedWireBounds {
                table: "accounts",
                write_events: expected_writes,
                annotation_bytes: ANNOTATION_BYTES,
                xid_end,
            },
        )
    }

    #[test]
    fn accepts_exact_source_table_map_insert_and_approved_xid() {
        let mut events = valid_events().into_iter();
        assert!(validate(&mut events, 1, XID_END).is_ok());
    }

    #[test]
    fn rejects_wrong_server_schema_and_table_map() {
        for first in [
            (header(SERVER_ID + 1, 0), table_map("app", "accounts")),
            (header(SERVER_ID, 0), table_map("other", "accounts")),
            (header(SERVER_ID, 0), table_map("app", "other")),
        ] {
            let mut events = vec![
                Ok(first),
                Ok((header(SERVER_ID, 0), write(TABLE_ID, vec![row()]))),
                Ok((header(SERVER_ID, XID_END), xid())),
            ]
            .into_iter();
            assert!(validate(&mut events, 1, XID_END).is_err());
        }
    }

    #[test]
    fn rejects_wrong_write_count_and_xid_end() {
        let mut too_few = valid_events().into_iter();
        assert!(validate(&mut too_few, 2, XID_END).is_err());

        let mut wrong_end = valid_events().into_iter();
        assert!(validate(&mut wrong_end, 1, XID_END - 1).is_err());
        let mut wrong_gap = valid_events().into_iter();
        let bounds = ApprovedWireBounds {
            table: "accounts",
            write_events: 1,
            annotation_bytes: ANNOTATION_BYTES + 1,
            xid_end: XID_END,
        };
        assert!(validate_source_only_group(&mut wrong_gap, &ddl(), bounds).is_err());
    }

    #[test]
    fn rejects_mixed_events_empty_rows_and_unsafe_flags() {
        let mut mixed = vec![
            Ok((header(SERVER_ID, 0), table_map("app", "accounts"))),
            Ok((
                header(SERVER_ID, 0),
                BinlogEvent::DeleteRowsEvent(
                    mysql_cdc::events::row_events::delete_rows_event::DeleteRowsEvent {
                        table_id: TABLE_ID,
                        flags: 0,
                        columns_number: 1,
                        columns_present: vec![true],
                        rows: vec![row()],
                    },
                ),
            )),
            Ok((header(SERVER_ID, XID_END), xid())),
        ]
        .into_iter();
        assert!(validate(&mut mixed, 1, XID_END).is_err());

        let mut mixed_table = vec![
            Ok((header(SERVER_ID, 0), table_map("app", "accounts"))),
            Ok((header(SERVER_ID, 0), write(TABLE_ID + 1, vec![row()]))),
            Ok((header(SERVER_ID, XID_END), xid())),
        ]
        .into_iter();
        assert!(validate(&mut mixed_table, 1, XID_END).is_err());

        let mut empty = vec![
            Ok((header(SERVER_ID, 0), table_map("app", "accounts"))),
            Ok((header(SERVER_ID, 0), write(TABLE_ID, vec![]))),
            Ok((header(SERVER_ID, XID_END), xid())),
        ]
        .into_iter();
        assert!(validate(&mut empty, 1, XID_END).is_err());

        let mut unsafe_header = valid_events();
        unsafe_header[0].as_mut().unwrap().0.event_flags = 1;
        let mut unsafe_header = unsafe_header.into_iter();
        assert!(validate(&mut unsafe_header, 1, XID_END).is_err());

        let mut unsafe_rows = valid_events();
        let BinlogEvent::WriteRowsEvent(write) = &mut unsafe_rows[1].as_mut().unwrap().1 else {
            unreachable!();
        };
        write.flags = 2;
        let mut unsafe_rows = unsafe_rows.into_iter();
        assert!(validate(&mut unsafe_rows, 1, XID_END).is_err());
    }

    #[test]
    fn does_not_read_past_the_approved_xid() {
        let mut events = valid_events();
        events.push(Ok((header(SERVER_ID, XID_END + 1), xid())));
        let mut events = events.into_iter();
        assert!(validate(&mut events, 1, XID_END).is_ok());
        assert!(events.next().is_some());
    }
}
