use super::*;
use mysql::Value;
use mysql_cdc::binlog_reader::BinlogReader;
use mysql_cdc::events::event_header::EventHeader;
use mysql_cdc::events::query_event::QueryEvent;
use mysql_cdc::events::rotate_event::RotateEvent;
use mysql_cdc::events::row_events::row_data::{RowData, UpdateRowData};
use mysql_cdc::events::row_events::update_rows_event::UpdateRowsEvent as MysqlCdcUpdateRowsEvent;
use mysql_cdc::events::row_events::write_rows_event::WriteRowsEvent as MysqlCdcWriteRowsEvent;
use mysql_cdc::events::rows_query_event::RowsQueryEvent;
use mysql_cdc::events::xid_event::XidEvent;
use mysql_cdc::starting_strategy::StartingStrategy;
use std::fs::File;

mod ddl_checkpoint;
mod ddl_replay;
mod event;
mod init;
mod schema;
mod token;
mod transaction;
mod value;

fn convert_mysql_value(value: &Option<MySqlValue>, signed: bool) -> Value {
    mysql_value_to_target_value(value, signed, None).expect("convert mysql value")
}

fn fixture_events(path: &str) -> Vec<(EventHeader, BinlogEvent)> {
    let file = File::open(path).expect("open fixture");
    let reader = BinlogReader::new(file).expect("create binlog reader");
    reader
        .read_events()
        .map(|event| event.expect("fixture event"))
        .collect()
}

fn write_rows_event(table_id: u64, id: u32, name: &str) -> BinlogEvent {
    BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id,
        flags: 0,
        columns_number: 5,
        columns_present: vec![true; 5],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(id)),
            Some(MySqlValue::String(name.to_string())),
            Some(MySqlValue::Int(100)),
            Some(MySqlValue::String("safe".to_string())),
            Some(MySqlValue::DateTime(DateTime {
                year: 2026,
                month: 6,
                day: 22,
                hour: 12,
                minute: 3,
                second: 4,
                millis: 0,
            })),
        ])],
    })
}

fn accounts_row_table_map() -> crate::row::TableMapEvent {
    crate::row::TableMapEvent {
        coordinate: stream_coordinate(200),
        table: crate::row::RowTableMap {
            table_id: 18,
            schema: "fixture_cdc".to_string(),
            table: "accounts".to_string(),
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "balance".to_string(),
                "note".to_string(),
                "created_at".to_string(),
            ],
            primary_key: vec!["id".to_string()],
            generated_columns: Vec::new(),
            signed_columns: Vec::new(),
            enum_columns: BTreeMap::new(),
        },
    }
}

fn accounts_table_map_event(column_count: usize) -> MysqlCdcTableMapEvent {
    MysqlCdcTableMapEvent {
        table_id: 18,
        database_name: "fixture_cdc".to_string(),
        table_name: "accounts".to_string(),
        column_types: vec![3; column_count],
        column_metadata: vec![0; column_count],
        null_bitmap: vec![false; column_count],
        table_metadata: None,
    }
}

fn stream_coordinate(position: u64) -> BinlogCoordinate {
    BinlogCoordinate {
        file: "mysql-bin.000001".to_string(),
        position,
    }
}

fn bytes(item: &str) -> Value {
    Value::Bytes(item.as_bytes().to_vec())
}

fn event_header(event_type: u8, next_event_position: u32) -> EventHeader {
    EventHeader {
        timestamp: 0,
        event_type,
        server_id: 1,
        event_length: 19,
        next_event_position,
        event_flags: 0,
    }
}

struct FixtureSchemaResolver;

impl TableSchemaResolver for FixtureSchemaResolver {
    fn resolve_table_schema(
        &self,
        schema: &str,
        table: &str,
        column_count: usize,
    ) -> Result<ResolvedTableSchema, ApplyBinlogError> {
        assert_eq!(schema, "fixture_cdc");
        fixture_table_schema(table, column_count)
    }
}

fn fixture_table_schema(
    table: &str,
    column_count: usize,
) -> Result<ResolvedTableSchema, ApplyBinlogError> {
    match (table, column_count) {
        ("audit_log", 3) => Ok(schema(vec!["id", "account_id", "message"])),
        ("accounts", 5) => Ok(schema(vec!["id", "name", "balance", "note", "created_at"])),
        ("accounts", 6) => Ok(schema(vec![
            "id",
            "name",
            "balance",
            "uuid",
            "created_at",
            "status",
        ])),
        _ => Err(mapping_error(format!(
            "unexpected fixture table {table}/{column_count}"
        ))),
    }
}

fn schema(columns: Vec<&str>) -> ResolvedTableSchema {
    ResolvedTableSchema {
        columns: columns.into_iter().map(str::to_string).collect(),
        primary_key: vec!["id".to_string()],
        generated_columns: Vec::new(),
        signed_columns: Vec::new(),
        enum_columns: BTreeMap::new(),
    }
}

struct ReleasesSchemaResolver;

impl TableSchemaResolver for ReleasesSchemaResolver {
    fn resolve_table_schema(
        &self,
        schema: &str,
        table: &str,
        column_count: usize,
    ) -> Result<ResolvedTableSchema, ApplyBinlogError> {
        assert_eq!(schema, "app");
        assert_eq!(table, "releases");
        assert_eq!(column_count, 2);
        Ok(ResolvedTableSchema {
            columns: vec!["id".to_string(), "public_time_delta".to_string()],
            primary_key: vec!["id".to_string()],
            generated_columns: Vec::new(),
            signed_columns: Vec::new(),
            enum_columns: BTreeMap::from([(
                "public_time_delta".to_string(),
                vec!["1".to_string(), "2".to_string(), "14".to_string()],
            )]),
        })
    }
}

struct NoopCheckpointStore;

struct RecordingCheckpointStore {
    operations: std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>,
}

impl RecordingCheckpointStore {
    fn new(operations: std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>) -> Self {
        Self { operations }
    }
}

impl StreamCheckpointStore for RecordingCheckpointStore {
    fn load_checkpoint(&self) -> Result<Option<crate::checkpoint::Checkpoint>, ApplyBinlogError> {
        Ok(None)
    }

    fn save_checkpoint(
        &self,
        _checkpoint: &crate::checkpoint::Checkpoint,
    ) -> Result<(), ApplyBinlogError> {
        self.operations.borrow_mut().push("CHECKPOINT");
        Ok(())
    }
}

impl StreamCheckpointStore for NoopCheckpointStore {
    fn load_checkpoint(&self) -> Result<Option<crate::checkpoint::Checkpoint>, ApplyBinlogError> {
        Ok(None)
    }

    fn save_checkpoint(
        &self,
        _checkpoint: &crate::checkpoint::Checkpoint,
    ) -> Result<(), ApplyBinlogError> {
        Ok(())
    }
}

struct EmptySchemaResolver;

impl TableSchemaResolver for EmptySchemaResolver {
    fn resolve_table_schema(
        &self,
        schema: &str,
        table: &str,
        _column_count: usize,
    ) -> Result<ResolvedTableSchema, ApplyBinlogError> {
        Err(mapping_error(format!(
            "unexpected fallback for {schema}.{table}"
        )))
    }
}

struct RecordingSemanticInventory {
    evidence: super::super::ddl_semantics::DdlSemanticEvidence,
    observed_state: String,
    capture_error: Option<String>,
    translator_available: std::cell::Cell<bool>,
    use_live_transform: bool,
    present_target_create_evidence: bool,
    absent_target_create_evidence: bool,
    create_observed_state: std::cell::RefCell<Option<String>>,
}

impl Default for RecordingSemanticInventory {
    fn default() -> Self {
        Self {
            evidence: super::super::ddl_semantics::DdlSemanticEvidence {
                transformation_version: "test-v1".to_string(),
                generated_sql: Some("translated DDL".to_string()),
                canonical_ast: "{\"family\":\"table\"}".to_string(),
                pre_state: "before".to_string(),
                expected_post_state: "after".to_string(),
            },
            observed_state: "after".to_string(),
            capture_error: None,
            translator_available: std::cell::Cell::new(true),
            use_live_transform: false,
            present_target_create_evidence: false,
            absent_target_create_evidence: false,
            create_observed_state: std::cell::RefCell::new(None),
        }
    }
}

impl super::super::ddl_semantics::DdlSemanticInventory for RecordingSemanticInventory {
    fn transform_sql(
        &self,
        sql: &str,
    ) -> Result<super::super::ddl_semantics::DdlTransformation, String> {
        if self.use_live_transform {
            let live = super::super::ddl_semantics::LiveDdlSemanticInventory::new(
                crate::inventory::InventoryConfig::default(),
                crate::inventory::InventoryConfig::default(),
                "fixture_cdc".to_string(),
                "fixture_cdc".to_string(),
            );
            return super::super::ddl_semantics::DdlSemanticInventory::transform_sql(&live, sql);
        }
        if !self.translator_available.get() {
            return Err("translator implementation unavailable".to_string());
        }
        let target_sql = if sql.to_ascii_uppercase().contains("RENAME COLUMN IF EXISTS") {
            Some(
                "ALTER TABLE `home_feed_captions` RENAME COLUMN `arc_start_order` TO `deprecated_arc_start_order`"
                    .to_string(),
            )
        } else {
            Some(sql.to_string())
        };
        Ok(super::super::ddl_semantics::DdlTransformation {
            version: "test-v1",
            target_sql,
        })
    }

    fn capture_evidence(
        &self,
        sql: &str,
        source_file: &str,
        event_end_position: u64,
    ) -> Result<super::super::ddl_semantics::DdlSemanticEvidence, String> {
        if self.present_target_create_evidence || self.absent_target_create_evidence {
            let operation = super::super::ddl_semantics::parse_ddl_operation(sql)?;
            let tables = if self.present_target_create_evidence {
                vec![crate::inventory::TableInventory {
                    name: "accounts".to_string(),
                    table_type: "BASE TABLE".to_string(),
                    engine: Some("InnoDB".to_string()),
                    collation: Some("utf8mb4_unicode_ci".to_string()),
                    primary_key: vec!["id".to_string()],
                    columns: Vec::new(),
                }]
            } else {
                Vec::new()
            };
            let target = super::super::ddl_semantics::SemanticSchemaSnapshot {
                inventory: crate::inventory::SchemaInventory {
                    schema: "fixture_cdc".to_string(),
                    tables,
                    indexes: Vec::new(),
                    foreign_keys: Vec::new(),
                    views: Vec::new(),
                    triggers: Vec::new(),
                    routines: Vec::new(),
                    events: Vec::new(),
                },
                table_runtime: Default::default(),
            };
            let coordinate = crate::inventory::SourceMasterCoordinate {
                file: source_file.to_string(),
                position: event_end_position,
            };
            let evidence = super::super::ddl_semantics::build_fenced_create_table_evidence(
                &operation,
                &target,
                &crate::inventory::SchemaDefaults {
                    character_set: "utf8mb4".to_string(),
                    collation: "utf8mb4_unicode_ci".to_string(),
                },
                source_file,
                event_end_position,
                &coordinate,
                &coordinate,
            )?;
            self.create_observed_state
                .replace(Some(evidence.expected_post_state.clone()));
            return Ok(evidence);
        }
        match &self.capture_error {
            Some(error) => Err(error.clone()),
            None => Ok(self.evidence.clone()),
        }
    }

    fn observe_target_state(&self, _sql: &str) -> Result<String, String> {
        Ok(self
            .create_observed_state
            .borrow()
            .clone()
            .unwrap_or_else(|| self.observed_state.clone()))
    }
}

#[derive(Default)]
struct RecordingDdlReplayJournal {
    status: RefCell<Option<DdlReplayStatus>>,
    evidence: RefCell<Option<super::super::ddl_semantics::DdlSemanticEvidence>>,
    operations: std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>,
}

impl RecordingDdlReplayJournal {
    fn with_operations(operations: std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>) -> Self {
        Self {
            status: RefCell::new(None),
            evidence: RefCell::new(None),
            operations,
        }
    }
}

impl DdlReplayJournal for RecordingDdlReplayJournal {
    fn ensure(&self) -> Result<(), String> {
        Ok(())
    }

    fn earliest_barrier(
        &self,
        _source_identity: &str,
    ) -> Result<Option<super::super::ddl_replay_journal::JournalBarrier>, String> {
        Ok(None)
    }

    fn read_status(&self, _event: &DdlEvent) -> Result<Option<DdlReplayStatus>, String> {
        Ok(*self.status.borrow())
    }

    fn read_evidence(
        &self,
        _event: &DdlEvent,
    ) -> Result<Option<super::super::ddl_semantics::DdlSemanticEvidence>, String> {
        Ok(self.evidence.borrow().clone())
    }

    fn record_translation_pending(&self, _event: &DdlEvent) -> Result<(), String> {
        self.operations.borrow_mut().push("TRANSLATION_PENDING");
        *self.status.borrow_mut() = Some(DdlReplayStatus::TranslationPending);
        Ok(())
    }

    fn prepare(
        &self,
        _event: &DdlEvent,
        evidence: &super::super::ddl_semantics::DdlSemanticEvidence,
    ) -> Result<(), String> {
        let operation = if *self.status.borrow() == Some(DdlReplayStatus::TranslationPending) {
            "PROMOTE"
        } else {
            "PREPARE"
        };
        self.operations.borrow_mut().push(operation);
        *self.status.borrow_mut() = Some(DdlReplayStatus::Prepared);
        *self.evidence.borrow_mut() = Some(evidence.clone());
        Ok(())
    }

    fn mark_applied(&self, _event: &DdlEvent) -> Result<(), String> {
        self.operations.borrow_mut().push("APPLIED");
        *self.status.borrow_mut() = Some(DdlReplayStatus::Applied);
        Ok(())
    }

    fn mark_blocked(&self, _event: &DdlEvent) -> Result<(), String> {
        self.operations.borrow_mut().push("BLOCKED");
        *self.status.borrow_mut() = Some(DdlReplayStatus::Blocked);
        Ok(())
    }

    fn checkpoint_transition_statement(
        &self,
        _event: &DdlEvent,
    ) -> Result<crate::target::SqlStatement, String> {
        Ok(crate::target::SqlStatement {
            sql: "UPDATE cdc.ddl_replay_journal SET status='checkpointed'".to_string(),
            params: Vec::new(),
        })
    }
}

#[derive(Clone, Copy)]
enum DuplicateMode {
    Equal,
    Divergent,
    DefaultError,
    ForeignKey,
    UpdateUnique,
}

struct TransactionRecordingExecutor {
    operations: std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>,
    fail_execute: bool,
    duplicate_row_change_number: Option<usize>,
    duplicate_mode: DuplicateMode,
    row_change_count: std::cell::Cell<usize>,
    locked_checkpoint: Option<crate::checkpoint::Checkpoint>,
}

impl Default for TransactionRecordingExecutor {
    fn default() -> Self {
        Self {
            operations: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            fail_execute: false,
            duplicate_row_change_number: None,
            duplicate_mode: DuplicateMode::Equal,
            row_change_count: std::cell::Cell::new(0),
            locked_checkpoint: Some(crate::checkpoint::Checkpoint {
                source_file: "mysqld-bin.000000".to_string(),
                source_position: 4,
                gtid: None,
                event_timestamp: 0,
                last_event: crate::checkpoint::LastEvent {
                    event_type: "Bootstrap".to_string(),
                    description: "test checkpoint".to_string(),
                },
            }),
        }
    }
}

impl TransactionRecordingExecutor {
    fn with_operations(operations: std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>) -> Self {
        Self {
            operations,
            fail_execute: false,
            duplicate_row_change_number: None,
            duplicate_mode: DuplicateMode::Equal,
            row_change_count: std::cell::Cell::new(0),
            locked_checkpoint: Some(crate::checkpoint::Checkpoint {
                source_file: "mysqld-bin.000777".to_string(),
                source_position: 161,
                gtid: None,
                event_timestamp: 0,
                last_event: crate::checkpoint::LastEvent {
                    event_type: "Bootstrap".to_string(),
                    description: "automatic DDL predecessor".to_string(),
                },
            }),
        }
    }

    fn failing() -> Self {
        Self {
            operations: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            fail_execute: true,
            duplicate_row_change_number: None,
            duplicate_mode: DuplicateMode::DefaultError,
            row_change_count: std::cell::Cell::new(0),
            locked_checkpoint: None,
        }
    }

    fn with_locked_checkpoint(checkpoint: crate::checkpoint::Checkpoint) -> Self {
        Self {
            locked_checkpoint: Some(checkpoint),
            ..Self::default()
        }
    }

    fn with_equal_duplicate_second_row_change() -> Self {
        Self {
            duplicate_row_change_number: Some(2),
            duplicate_mode: DuplicateMode::Equal,
            ..Self::default()
        }
    }

    fn with_divergent_duplicate_second_row_change() -> Self {
        Self {
            duplicate_row_change_number: Some(1),
            duplicate_mode: DuplicateMode::Divergent,
            ..Self::default()
        }
    }

    fn with_default_duplicate_second_row_change() -> Self {
        Self {
            duplicate_row_change_number: Some(1),
            duplicate_mode: DuplicateMode::DefaultError,
            ..Self::default()
        }
    }

    fn with_foreign_key_conflict_second_row_change() -> Self {
        Self {
            duplicate_row_change_number: Some(1),
            duplicate_mode: DuplicateMode::ForeignKey,
            ..Self::default()
        }
    }

    fn with_update_unique_conflict() -> Self {
        Self {
            duplicate_row_change_number: Some(1),
            duplicate_mode: DuplicateMode::UpdateUnique,
            ..Self::default()
        }
    }

    fn operations(&self) -> Vec<&'static str> {
        self.operations.borrow().clone()
    }

    fn shared_operations(&self) -> std::rc::Rc<std::cell::RefCell<Vec<&'static str>>> {
        std::rc::Rc::clone(&self.operations)
    }
}

impl TargetExecutor for TransactionRecordingExecutor {
    fn execute(
        &self,
        _statement: &crate::target::SqlStatement,
    ) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("EXEC");
        if self.fail_execute {
            return Err(crate::target::TargetExecuteError::new("forced failure"));
        }
        Ok(())
    }

    fn execute_row_change(
        &self,
        change: &crate::target::TargetRowChange,
    ) -> Result<crate::target::TargetExecutionOutcome, crate::target::TargetExecuteError> {
        self.execute(&change.statement)?;
        let row_change_number = self.row_change_count.get() + 1;
        self.row_change_count.set(row_change_number);
        if self
            .duplicate_row_change_number
            .is_some_and(|interval| row_change_number.is_multiple_of(interval))
        {
            let conflict = crate::target::DuplicateConflict {
                error_code: 1062,
                error_text: "Duplicate entry for key 'PRIMARY'".to_string(),
                duplicate_index: Some("PRIMARY".to_string()),
            };
            return match self.duplicate_mode {
                DuplicateMode::Equal => Ok(crate::target::duplicate_insert_outcome(
                    conflict,
                    Some(&change.source_values),
                    &change.source_values,
                )),
                DuplicateMode::Divergent => Ok(crate::target::duplicate_insert_outcome(
                    conflict,
                    Some(&[Value::Bytes(b"different".to_vec())]),
                    &change.source_values,
                )),
                DuplicateMode::DefaultError => Err(crate::target::TargetExecuteError::from_mysql(
                    1062,
                    "ERROR 1062 duplicate entry for key 'PRIMARY'",
                )),
                DuplicateMode::ForeignKey => {
                    Ok(crate::target::TargetExecutionOutcome::ConstraintConflict(
                        crate::target::DuplicateConflict {
                            error_code: 1452,
                            error_text:
                                "Cannot add or update a child row: a foreign key constraint fails"
                                    .to_string(),
                            duplicate_index: None,
                        },
                    ))
                }
                DuplicateMode::UpdateUnique => {
                    Ok(crate::target::TargetExecutionOutcome::ConstraintConflict(
                        crate::target::DuplicateConflict {
                            error_code: 1062,
                            error_text: "Duplicate entry 'beta' for key 'uq_accounts_name'"
                                .to_string(),
                            duplicate_index: Some("uq_accounts_name".to_string()),
                        },
                    ))
                }
            };
        }
        Ok(crate::target::TargetExecutionOutcome::Applied)
    }
}

impl crate::target::TransactionalTargetExecutor for TransactionRecordingExecutor {
    fn acquire_stream_lease(
        &self,
        _lease_name: &str,
    ) -> Result<(), crate::target::TargetExecuteError> {
        Ok(())
    }

    fn begin_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("BEGIN");
        Ok(())
    }

    fn load_transaction_checkpoint_for_update(
        &self,
        _checkpoint_table: &str,
        _checkpoint_name: &str,
    ) -> Result<Option<crate::checkpoint::Checkpoint>, crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("LOCK_CHECKPOINT");
        Ok(self.locked_checkpoint.clone())
    }

    fn save_transaction_checkpoint(
        &self,
        _checkpoint_table: &str,
        _checkpoint_name: &str,
        _checkpoint: &crate::checkpoint::Checkpoint,
    ) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("CHECKPOINT");
        Ok(())
    }

    fn commit_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("COMMIT");
        Ok(())
    }

    fn rollback_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("ROLLBACK");
        Ok(())
    }
}

#[derive(Default)]
struct RecordingExecutor {
    statements: RefCell<Vec<crate::target::SqlStatement>>,
}

impl TargetExecutor for RecordingExecutor {
    fn execute(
        &self,
        statement: &crate::target::SqlStatement,
    ) -> Result<(), crate::target::TargetExecuteError> {
        self.statements.borrow_mut().push(statement.clone());
        Ok(())
    }
}
