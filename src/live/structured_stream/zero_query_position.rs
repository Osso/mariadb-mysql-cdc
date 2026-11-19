use mysql_cdc::constants::EVENT_HEADER_SIZE;
use mysql_cdc::events::{binlog_event::BinlogEvent, event_header::EventHeader};

const FIRST_BINLOG_EVENT: u32 = 4;
const ARTIFICIAL_EVENT: u16 = 0x20;

#[derive(Default)]
pub(super) struct ZeroQueryPosition {
    previous_gtid: Option<(u32, u32)>,
}

impl ZeroQueryPosition {
    pub(super) fn observe(&mut self, header: &EventHeader, event: &BinlogEvent) -> Option<u32> {
        let previous = self.previous_gtid.take();
        if matches!(event, BinlogEvent::MariaDbGtidEvent(_)) && physical_event(header) {
            let start = header
                .next_event_position
                .checked_sub(header.event_length)?;
            if start >= FIRST_BINLOG_EVENT {
                self.previous_gtid = Some((header.server_id, header.next_event_position));
            }
        }
        if header.next_event_position != 0 || !matches!(event, BinlogEvent::QueryEvent(_)) {
            return None;
        }
        let (server_id, start) = previous?;
        if header.server_id != server_id || !physical_event(header) {
            return None;
        }
        start.checked_add(header.event_length)
    }
}

fn physical_event(header: &EventHeader) -> bool {
    header.timestamp != 0
        && header.event_flags & ARTIFICIAL_EVENT == 0
        && header.event_length >= EVENT_HEADER_SIZE as u32
}
