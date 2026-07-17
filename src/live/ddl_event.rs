#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlEvent {
    pub source_identity: String,
    pub source_server_id: u32,
    pub binlog_file: String,
    pub event_start_position: u64,
    pub event_end_position: u64,
    pub schema_name: String,
    pub raw_sql: String,
}
