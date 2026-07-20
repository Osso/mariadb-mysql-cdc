use super::model::*;
use super::schema::*;
use super::sql::*;
use crate::mysql_support::quote_sql_literal;
use crate::snapshot::SnapshotRow;
use mysql::Conn;
use mysql::prelude::Queryable;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

pub trait ConflictStore {
    fn ensure(&mut self) -> Result<(), String> {
        Ok(())
    }
    fn observe(&mut self, observation: ConflictObservation) -> Result<(), String>;
    fn resolve_existing(&mut self, resolution: ConflictResolution) -> Result<(), String>;
    fn resolution_sql(&self, resolution: &ConflictResolution) -> String;
    fn mark_resolution_committed(&mut self, resolution: ConflictResolution);
    fn has_unresolved(&mut self, resolution: &ConflictResolution) -> Result<bool, String>;

    fn resolve_if_equal(
        &mut self,
        table: &str,
        primary_key: &[String],
        rows_equal: bool,
        repair_run_id: &str,
        evidence: &str,
    ) -> Result<(), String>;
    fn unresolved_count(&self) -> usize;

    fn unresolved_count_result(&mut self) -> Result<usize, String> {
        Ok(self.unresolved_count())
    }
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryConflictStore {
    records: BTreeMap<ConflictKey, RowConflictRecord>,
}

impl InMemoryConflictStore {
    pub fn records(&self) -> Vec<RowConflictRecord> {
        self.records.values().cloned().collect()
    }

    fn mark_matching_resolution(&mut self, resolution: ConflictResolution) {
        let Some(record) = self.records.values_mut().find(|record| {
            record.status == ConflictStatus::Unresolved
                && conflict_key_matches_resolution(&record.key, &resolution)
        }) else {
            return;
        };
        record.status = ConflictStatus::Resolved;
        record.repair_run_id = Some(resolution.repair_run_id);
        record.resolution_evidence = Some(resolution.evidence);
    }
}

fn conflict_key_matches_resolution(key: &ConflictKey, resolution: &ConflictResolution) -> bool {
    let has_same_source_identity = key.source_identity == resolution.source_identity;
    let has_same_table = key.schema == resolution.schema && key.table == resolution.table;
    let has_same_primary_key = key.source_primary_key == resolution.source_primary_key;

    has_same_source_identity && has_same_table && has_same_primary_key
}

impl ConflictStore for InMemoryConflictStore {
    fn has_unresolved(&mut self, resolution: &ConflictResolution) -> Result<bool, String> {
        Ok(self.records.values().any(|record| {
            record.status == ConflictStatus::Unresolved
                && conflict_key_matches_resolution(&record.key, resolution)
        }))
    }

    fn resolve_existing(&mut self, resolution: ConflictResolution) -> Result<(), String> {
        self.mark_matching_resolution(resolution);
        Ok(())
    }

    fn resolution_sql(&self, resolution: &ConflictResolution) -> String {
        build_conflict_resolution_for_source_row_sql("cdc.row_conflicts", resolution)
    }

    fn mark_resolution_committed(&mut self, resolution: ConflictResolution) {
        self.mark_matching_resolution(resolution);
    }

    fn observe(&mut self, observation: ConflictObservation) -> Result<(), String> {
        let key = observation.key();
        if let Some(record) = self.records.get_mut(&key) {
            record.duplicate_index = observation.duplicate_index;
            record.duplicate_owner_primary_key = observation.duplicate_owner_primary_key;
            record.error_code = observation.error_code;
            record.error_text = observation.error_text;
            record.last_observed_at_ms = observation.observed_at_ms;
            record.attempt_count += 1;
            return Ok(());
        }
        self.records.insert(
            key.clone(),
            RowConflictRecord {
                key,
                duplicate_index: observation.duplicate_index,
                duplicate_owner_primary_key: observation.duplicate_owner_primary_key,
                error_code: observation.error_code,
                error_text: observation.error_text,
                first_observed_at_ms: observation.observed_at_ms,
                last_observed_at_ms: observation.observed_at_ms,
                attempt_count: 1,
                status: ConflictStatus::Unresolved,
                repair_run_id: None,
                resolution_evidence: None,
            },
        );
        Ok(())
    }

    fn resolve_if_equal(
        &mut self,
        table: &str,
        primary_key: &[String],
        rows_equal: bool,
        repair_run_id: &str,
        evidence: &str,
    ) -> Result<(), String> {
        if !rows_equal {
            return Ok(());
        }
        for record in self.records.values_mut().filter(|record| {
            record.key.table == table
                && record.key.source_primary_key.as_slice() == primary_key
                && record.status == ConflictStatus::Unresolved
        }) {
            record.status = ConflictStatus::Resolved;
            record.repair_run_id = Some(repair_run_id.to_string());
            record.resolution_evidence = Some(evidence.to_string());
        }
        Ok(())
    }

    fn unresolved_count(&self) -> usize {
        self.records
            .values()
            .filter(|record| record.status == ConflictStatus::Unresolved)
            .count()
    }
}
pub trait RepairProgressStore {
    fn load(&self, run_id: &str) -> Result<Option<RepairRunState>, String>;
    fn save(&mut self, state: &RepairRunState) -> Result<(), String>;
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryRepairProgressStore {
    states: BTreeMap<String, RepairRunState>,
}

impl RepairProgressStore for InMemoryRepairProgressStore {
    fn load(&self, run_id: &str) -> Result<Option<RepairRunState>, String> {
        Ok(self.states.get(run_id).cloned())
    }

    fn save(&mut self, state: &RepairRunState) -> Result<(), String> {
        self.states.insert(state.run_id.clone(), state.clone());
        Ok(())
    }
}

pub trait RepairExecutor {
    fn target_rows(&self, table: &str) -> Vec<SnapshotRow>;
    fn apply(&mut self, operation: &RepairOperation) -> Result<(), String>;
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryRepairExecutor {
    rows: BTreeMap<String, Vec<SnapshotRow>>,
    pub operations: Vec<RepairOperation>,
    pub fail_after_operations: Option<usize>,
}

impl InMemoryRepairExecutor {
    pub fn from_rows(rows: BTreeMap<String, Vec<SnapshotRow>>) -> Self {
        Self {
            rows,
            ..Self::default()
        }
    }

    pub fn rows(&self) -> BTreeMap<String, Vec<SnapshotRow>> {
        self.rows.clone()
    }
}

impl RepairExecutor for InMemoryRepairExecutor {
    fn target_rows(&self, table: &str) -> Vec<SnapshotRow> {
        self.rows.get(table).cloned().unwrap_or_default()
    }

    fn apply(&mut self, operation: &RepairOperation) -> Result<(), String> {
        if self
            .fail_after_operations
            .is_some_and(|limit| self.operations.len() >= limit)
        {
            return Err("simulated repair crash".to_string());
        }
        apply_repair_operation(&mut self.rows, operation);
        self.operations.push(operation.clone());
        Ok(())
    }
}
fn apply_repair_operation(
    rows: &mut BTreeMap<String, Vec<SnapshotRow>>,
    operation: &RepairOperation,
) {
    match operation {
        RepairOperation::Delete { table, primary_key } => {
            if let Some(rows) = rows.get_mut(table) {
                rows.retain(|row| row.primary_key != *primary_key);
            }
        }
        RepairOperation::Insert { table, row } => {
            rows.entry(table.clone()).or_default().push(row.clone())
        }
        RepairOperation::Update { table, row } => update_repair_row(rows, table, row),
    }
}

fn update_repair_row(
    rows: &mut BTreeMap<String, Vec<SnapshotRow>>,
    table: &str,
    row: &SnapshotRow,
) {
    let table_rows = rows.entry(table.to_string()).or_default();
    if let Some(existing) = table_rows
        .iter_mut()
        .find(|existing| existing.primary_key == row.primary_key)
    {
        *existing = row.clone();
    } else {
        table_rows.push(row.clone());
    }
}

pub trait ConflictSqlExecutor {
    fn execute(&mut self, sql: &str) -> Result<(), String>;
}

pub struct MySqlConflictStore {
    conn: RefCell<Conn>,
    table: String,
    unresolved: RefCell<BTreeSet<ConflictKey>>,
}

impl MySqlConflictStore {
    pub fn new(
        target: &crate::live::TargetMySqlConfig,
        table: impl Into<String>,
    ) -> Result<Self, String> {
        let table = table.into();
        let options = crate::mysql_support::target_mysql_opts(target)?;
        let mut conn = Conn::new(options)
            .map_err(|error| format!("conflict store connect failed: {error}"))?;
        conn.query_drop(crate::live::target_session_init_command())
            .map_err(|error| format!("conflict store session initialization failed: {error}"))?;
        Ok(Self {
            conn: RefCell::new(conn),
            table,
            unresolved: RefCell::new(BTreeSet::new()),
        })
    }

    pub fn ensure(&self) -> Result<(), String> {
        let (schema, table) = split_conflict_table(&self.table)?;
        let mut conn = self.conn.borrow_mut();
        validate_conflict_columns(&query_conflict_columns(&mut conn, schema, table)?)
            .map_err(conflict_validation_error)?;
        validate_conflict_identity_definition(&query_identity_definition(
            &mut conn, schema, table,
        )?)
        .map_err(conflict_validation_error)?;
        validate_conflict_keys(&query_conflict_keys(&mut conn, schema, table)?)
            .map_err(conflict_validation_error)?;
        validate_conflict_constraints(&query_conflict_constraints(&mut conn, schema, table)?)
            .map_err(conflict_validation_error)?;
        validate_conflict_status_checks(&query_conflict_checks(&mut conn, schema, table)?)
            .map_err(conflict_validation_error)?;
        let triggers = query_conflict_trigger_inventory(&mut conn, &self.table)?;
        validate_conflict_triggers(schema, table, &triggers).map_err(conflict_validation_error)
    }

    pub fn unresolved_count(&self) -> usize {
        self.unresolved.borrow().len()
    }

    pub fn resolve_verified_table(
        &mut self,
        source_identity: &str,
        table: &str,
        repair_run_id: &str,
        evidence: &str,
    ) -> Result<(), String> {
        self.validate_unresolved_rows(Some(source_identity), Some(table))?;
        self.conn
            .borrow_mut()
            .query_drop(build_conflict_table_resolution_sql(
                &self.table,
                source_identity,
                table,
                repair_run_id,
                evidence,
            ))
            .map_err(|error| format!("conflict store table resolution failed: {error}"))?;
        self.unresolved
            .borrow_mut()
            .retain(|key| !(key.source_identity == source_identity && key.table == table));
        Ok(())
    }

    pub fn unresolved_count_from_database(&self) -> Result<usize, String> {
        self.validate_unresolved_rows(None, None)?;
        let count = self
            .conn
            .borrow_mut()
            .query_first::<u64, _>(format!(
                "SELECT COUNT(*) FROM {} WHERE status='unresolved'",
                self.table
            ))
            .map_err(|error| format!("conflict store count query failed: {error}"))?
            .ok_or_else(|| "conflict store count query returned no row".to_string())?;
        usize::try_from(count).map_err(|_| "conflict store count exceeds usize".to_string())
    }

    fn validate_unresolved_rows(
        &self,
        source_identity: Option<&str>,
        table: Option<&str>,
    ) -> Result<(), String> {
        let mut query = format!(
            "SELECT conflict_identity,source_identity,source_server_id,source_file,source_start_position,schema_name,table_name,operation,source_primary_key_json FROM {} WHERE status='unresolved'",
            self.table
        );
        if let Some(source_identity) = source_identity {
            query.push_str(" AND source_identity=");
            query.push_str(&quote_sql_literal(source_identity));
        }
        if let Some(table) = table {
            query.push_str(" AND table_name=");
            query.push_str(&quote_sql_literal(table));
        }
        let rows = self
            .conn
            .borrow_mut()
            .query::<ConflictIdentityRow, _>(query)
            .map_err(|error| format!("conflict identity read failed: {error}"))?;
        for row in rows {
            validate_conflict_identity_row(&row)?;
        }
        Ok(())
    }
}

fn query_conflict_columns(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<ConflictColumn>, String> {
    conn.query(format!(
        "SELECT column_name,LOWER(column_type),is_nullable,LOWER(COALESCE(CAST(column_default AS CHAR),'<null>')),LOWER(extra) FROM information_schema.columns WHERE table_schema={} AND table_name={} ORDER BY ordinal_position",
        quote_sql_literal(schema), quote_sql_literal(table),
    )).map_err(conflict_mysql_error)
}

fn query_identity_definition(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<ConflictIdentityDefinition, String> {
    conn.query_first(format!(
        "SELECT LOWER(COALESCE(character_set_name,'')),LOWER(COALESCE(collation_name,'')) FROM information_schema.columns WHERE table_schema={} AND table_name={} AND column_name='conflict_identity'",
        quote_sql_literal(schema), quote_sql_literal(table),
    )).map_err(conflict_mysql_error)?.ok_or_else(|| "conflict identity column definition is missing".to_string())
}

fn query_conflict_keys(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<ConflictKeyIndex>, String> {
    conn.query(format!(
        "SELECT index_name,non_unique,seq_in_index,column_name,sub_part FROM information_schema.statistics WHERE table_schema={} AND table_name={} ORDER BY index_name,seq_in_index",
        quote_sql_literal(schema), quote_sql_literal(table),
    )).map_err(conflict_mysql_error)
}

fn query_conflict_constraints(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<ConflictConstraint>, String> {
    conn.query(format!(
        "SELECT constraint_type,enforced FROM information_schema.table_constraints WHERE table_schema={} AND table_name={} ORDER BY constraint_type,constraint_name",
        quote_sql_literal(schema), quote_sql_literal(table),
    )).map_err(conflict_mysql_error)
}

fn query_conflict_checks(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, String> {
    conn.query(format!(
        "SELECT cc.check_clause FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON cc.constraint_schema=tc.constraint_schema AND cc.constraint_name=tc.constraint_name WHERE tc.table_schema={} AND tc.table_name={} AND tc.constraint_type='CHECK' ORDER BY tc.constraint_name",
        quote_sql_literal(schema), quote_sql_literal(table),
    )).map_err(conflict_mysql_error)
}

impl ConflictStore for MySqlConflictStore {
    fn has_unresolved(&mut self, resolution: &ConflictResolution) -> Result<bool, String> {
        let primary_key_json = serde_json::to_string(&resolution.source_primary_key)
            .map_err(|error| format!("conflict primary key serialization failed: {error}"))?;
        self.conn
            .borrow_mut()
            .query_first::<u64, _>(format!(
                "SELECT COUNT(*) FROM {} WHERE source_identity={} AND schema_name={} AND table_name={} AND source_primary_key_json={} AND status='unresolved'",
                self.table,
                quote_sql_literal(&resolution.source_identity),
                quote_sql_literal(&resolution.schema),
                quote_sql_literal(&resolution.table),
                quote_sql_literal(&primary_key_json),
            ))
            .map_err(conflict_mysql_error)
            .map(|count| count.unwrap_or(0) > 0)
    }

    fn ensure(&mut self) -> Result<(), String> {
        Self::ensure(self)
    }

    fn resolve_existing(&mut self, resolution: ConflictResolution) -> Result<(), String> {
        self.conn
            .borrow_mut()
            .query_drop(build_conflict_resolution_for_source_row_sql(
                &self.table,
                &resolution,
            ))
            .map_err(|error| format!("conflict store resolution failed: {error}"))?;
        self.unresolved
            .borrow_mut()
            .retain(|key| !conflict_key_matches_resolution(key, &resolution));
        Ok(())
    }

    fn resolution_sql(&self, resolution: &ConflictResolution) -> String {
        build_conflict_resolution_for_source_row_sql(&self.table, resolution)
    }

    fn mark_resolution_committed(&mut self, resolution: ConflictResolution) {
        self.unresolved
            .borrow_mut()
            .retain(|key| !conflict_key_matches_resolution(key, &resolution));
    }

    fn observe(&mut self, observation: ConflictObservation) -> Result<(), String> {
        self.conn
            .borrow_mut()
            .query_drop(build_conflict_observation_sql(&self.table, &observation))
            .map_err(|error| format!("conflict store observation failed: {error}"))?;
        self.unresolved.borrow_mut().insert(observation.key());
        Ok(())
    }

    fn resolve_if_equal(
        &mut self,
        table: &str,
        primary_key: &[String],
        rows_equal: bool,
        repair_run_id: &str,
        evidence: &str,
    ) -> Result<(), String> {
        if !rows_equal {
            return Ok(());
        }
        self.conn
            .borrow_mut()
            .query_drop(build_conflict_resolution_by_table_sql(
                &self.table,
                table,
                primary_key,
                repair_run_id,
                evidence,
            ))
            .map_err(|error| format!("conflict store resolution failed: {error}"))?;
        self.unresolved.borrow_mut().retain(|key| {
            !(key.table == table && key.source_primary_key.as_slice() == primary_key)
        });
        Ok(())
    }

    fn unresolved_count(&self) -> usize {
        Self::unresolved_count(self)
    }

    fn unresolved_count_result(&mut self) -> Result<usize, String> {
        self.unresolved_count_from_database()
    }
}

pub struct DurableConflictStore<E> {
    pub(crate) executor: E,
    table: String,
    unresolved: BTreeSet<ConflictKey>,
}

impl<E: ConflictSqlExecutor> DurableConflictStore<E> {
    pub fn new(executor: E, table: impl Into<String>) -> Self {
        Self {
            executor,
            table: table.into(),
            unresolved: BTreeSet::new(),
        }
    }

    pub fn ensure(&mut self) -> Result<(), String> {
        self.executor
            .execute(&build_conflict_validation_sql(&self.table))
    }

    fn record_observation(&mut self, observation: &ConflictObservation) -> Result<(), String> {
        self.executor
            .execute(&build_conflict_observation_sql(&self.table, observation))?;
        self.unresolved.insert(observation.key());
        Ok(())
    }

    fn resolve_rows_equal(
        &mut self,
        table: &str,
        primary_key: &[String],
        rows_equal: bool,
        repair_run_id: &str,
        evidence: &str,
    ) -> Result<(), String> {
        if !rows_equal {
            return Ok(());
        }
        self.executor
            .execute(&build_conflict_resolution_by_table_sql(
                &self.table,
                table,
                primary_key,
                repair_run_id,
                evidence,
            ))?;
        self.unresolved.retain(|key| {
            !(key.table == table && key.source_primary_key.as_slice() == primary_key)
        });
        Ok(())
    }

    pub fn unresolved_count(&self) -> usize {
        self.unresolved.len()
    }
}

impl<E: ConflictSqlExecutor> ConflictStore for DurableConflictStore<E> {
    fn has_unresolved(&mut self, resolution: &ConflictResolution) -> Result<bool, String> {
        Ok(self.unresolved.iter().any(|key| {
            key.source_identity == resolution.source_identity
                && key.schema == resolution.schema
                && key.table == resolution.table
                && key.source_primary_key == resolution.source_primary_key
        }))
    }

    fn ensure(&mut self) -> Result<(), String> {
        DurableConflictStore::ensure(self)
    }

    fn resolve_existing(&mut self, resolution: ConflictResolution) -> Result<(), String> {
        self.executor.execute(
            build_conflict_resolution_for_source_row_sql(&self.table, &resolution).as_str(),
        )?;
        self.unresolved
            .retain(|key| !conflict_key_matches_resolution(key, &resolution));
        Ok(())
    }

    fn resolution_sql(&self, resolution: &ConflictResolution) -> String {
        build_conflict_resolution_for_source_row_sql(&self.table, resolution)
    }

    fn mark_resolution_committed(&mut self, resolution: ConflictResolution) {
        self.unresolved
            .retain(|key| !conflict_key_matches_resolution(key, &resolution));
    }

    fn observe(&mut self, observation: ConflictObservation) -> Result<(), String> {
        self.record_observation(&observation)
    }

    fn resolve_if_equal(
        &mut self,
        table: &str,
        primary_key: &[String],
        rows_equal: bool,
        repair_run_id: &str,
        evidence: &str,
    ) -> Result<(), String> {
        self.resolve_rows_equal(table, primary_key, rows_equal, repair_run_id, evidence)
    }

    fn unresolved_count(&self) -> usize {
        self.unresolved_count()
    }
}
