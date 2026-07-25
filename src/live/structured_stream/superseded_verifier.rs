use super::superseded_insert::{
    BinlogCoordinate, SupersededInsertProof, SupersededInsertVerificationInput,
    verify_superseded_insert,
};
use super::superseded_source::{
    SupersededSourceEvidence, build_exact_row_insert_statement,
    load_superseded_comics_source_evidence, load_superseded_source_evidence,
};
use super::transaction::SupersededInsertVerifier;
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::row::DeferredSupersededInsertCandidate;
use crate::target::{TransactionalTargetExecutor, UsersActiveTransactionEvidence};
use mysql::Value;

const USERS_SCHEMA: &str = "globalcomix";
const USERS_TABLE: &str = "users";
const USERS_NAME_INDEX: &str = "users.name";
const COMICS_TABLE: &str = "comics";
const COMICS_SLUG_INDEX: &str = "comics.slug";

pub(crate) struct ProductionSupersededInsertVerifier<'a, E> {
    source: MySqlConnectionConfig,
    target: &'a E,
    #[cfg(feature = "integration-failpoints")]
    logical_snapshot: Option<super::superseded_source::SourceSnapshotCoordinate>,
}

impl<'a, E> ProductionSupersededInsertVerifier<'a, E> {
    pub(crate) fn new(source: &MySqlConnectionConfig, target: &'a E) -> Self {
        Self {
            source: source.clone(),
            target,
            #[cfg(feature = "integration-failpoints")]
            logical_snapshot: None,
        }
    }

    #[cfg(feature = "integration-failpoints")]
    pub(crate) fn set_logical_snapshot(
        &mut self,
        snapshot: super::superseded_source::SourceSnapshotCoordinate,
    ) {
        self.logical_snapshot = Some(snapshot);
    }
}

impl<E> SupersededInsertVerifier for ProductionSupersededInsertVerifier<'_, E>
where
    E: TransactionalTargetExecutor,
{
    fn verify(
        &mut self,
        candidate: &DeferredSupersededInsertCandidate,
        xid_end_position: u64,
    ) -> Result<super::transaction::DeferredRepair, String> {
        // A foreign-key conflict is resolved from the locked parent, not from supersession hashes.
        if candidate.observation.error_code == FOREIGN_KEY_ERROR_CODE {
            return resolve_foreign_key_conflict(&self.source, self.target, candidate)
                .map(super::transaction::DeferredRepair::ForeignKey);
        }
        self.verify_superseded(candidate, xid_end_position)
            .map(super::transaction::DeferredRepair::Superseded)
    }
}

impl<E> ProductionSupersededInsertVerifier<'_, E>
where
    E: TransactionalTargetExecutor,
{
    fn verify_superseded(
        &mut self,
        candidate: &DeferredSupersededInsertCandidate,
        xid_end_position: u64,
    ) -> Result<SupersededInsertProof, String> {
        let is_comics = candidate.observation.table == COMICS_TABLE;
        let mut source = |primary_key: u64, identity: &str| {
            if is_comics {
                load_superseded_comics_source_evidence(&self.source, primary_key, identity)
            } else {
                load_superseded_source_evidence(&self.source, primary_key, identity)
            }
            .map_err(|error| error.to_string())
        };
        verify_with_source_loader(candidate, xid_end_position, &mut source, self.target)
    }
}

const FOREIGN_KEY_ERROR_CODE: u16 = 1452;

/// Resolves a deferred foreign-key conflict from the locked parent image.
///
/// The rejection text carries the `superseded ... insert rejected:` marker because
/// `superseded_verification_error` classifies fatal against retryable by that prefix; without it a
/// rejection crash-loops the stream instead of stalling it.
fn resolve_foreign_key_conflict<E>(
    source: &MySqlConnectionConfig,
    target: &E,
    candidate: &DeferredSupersededInsertCandidate,
) -> Result<super::foreign_key_repair::ForeignKeyRepairProof, String>
where
    E: TransactionalTargetExecutor,
{
    let change = &candidate.historical_change;
    let violation = crate::live::parse_foreign_key_violation(&candidate.observation.error_text)
        .ok_or_else(|| {
            "superseded foreign key insert rejected: error text did not name a foreign key"
                .to_string()
        })?;
    let foreign_key = super::foreign_key_repair::foreign_key_from_violation(&violation);
    let parent = crate::table_sync::read_parent_table_inventory(
        source,
        &foreign_key.referenced_schema,
        &foreign_key.referenced_table,
    )
    .map_err(|error| format!("superseded foreign key insert rejected: {error}"))?;
    let predicate = super::foreign_key_repair::parent_primary_key_predicate(
        &violation,
        &parent.primary_key,
        &change.writable_columns,
        &change.source_values,
    )
    .ok_or_else(|| {
        "superseded foreign key insert rejected: child image does not carry the referenced key"
            .to_string()
    })?;
    let locked_rows = target
        .read_locked_parent_identity(
            &foreign_key.referenced_schema,
            &foreign_key.referenced_table,
            &violation.parent_columns,
            &predicate,
        )
        .map_err(|error| format!("superseded foreign key target evidence failed: {error}"))?;
    let locked_parent = super::derived_fk_fastforward::LockedParentRows {
        columns: violation.parent_columns.clone(),
        rows: locked_rows,
    };
    let plan = super::foreign_key_repair::plan_foreign_key_repair(
        &super::foreign_key_repair::ForeignKeyRepairInput {
            violation: &violation,
            foreign_key: &foreign_key,
            operation: candidate.observation.operation,
            error_code: candidate.observation.error_code,
            parent_primary_key: &parent.primary_key,
            child_columns: &change.writable_columns,
            child_values: &change.source_values,
            locked_parent: &locked_parent,
        },
    )
    .map_err(|rejection| format!("superseded foreign key insert rejected: {rejection}"))?;
    match plan {
        super::foreign_key_repair::ForeignKeyRepairPlan::InstallParent => {
            install_parent_then_replay_child(source, &violation, candidate)
        }
        super::foreign_key_repair::ForeignKeyRepairPlan::FastForwardChild(plan) => {
            fast_forward_child(&violation, candidate, &plan)
        }
    }
}

/// Installs the exact source parent, then replays the child image unchanged.
fn install_parent_then_replay_child(
    source: &MySqlConnectionConfig,
    violation: &crate::live::ForeignKeyViolation,
    candidate: &DeferredSupersededInsertCandidate,
) -> Result<super::foreign_key_repair::ForeignKeyRepairProof, String> {
    let child_values = candidate
        .historical_change
        .source_values
        .iter()
        .cloned()
        .map(crate::mysql_client::value_to_string)
        .collect::<Vec<_>>();
    let referenced_values = violation
        .child_columns
        .iter()
        .map(|column| {
            let position = candidate
                .historical_change
                .writable_columns
                .iter()
                .position(|candidate| candidate == column)?;
            child_values.get(position).cloned()
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            "superseded foreign key insert rejected: child image lacks a referenced column"
                .to_string()
        })?;
    let (parent, parent_row) =
        crate::table_sync::read_exact_source_parent_row(source, violation, &referenced_values)
            .map_err(|error| format!("superseded foreign key insert rejected: {error}"))?;
    let parent_schema = violation
        .parent_schema
        .clone()
        .unwrap_or_else(|| violation.child_schema.clone());
    let parent_insert = build_exact_row_insert_statement(
        &parent_schema,
        &violation.parent_table,
        &snapshot_row_as_source_row(&parent_row, &parent),
    )
    .map_err(|error| format!("superseded foreign key insert rejected: {error}"))?;
    Ok(super::foreign_key_repair::ForeignKeyRepairProof {
        statements: vec![parent_insert, candidate.historical_change.statement.clone()],
        evidence: format!(
            "absent parent installed from the exact source row, then the historical child image \
             replayed unchanged; constraint={} parent=`{parent_schema}`.`{}` parent_key={:?}",
            violation.constraint, violation.parent_table, parent_row.primary_key,
        ),
    })
}

/// Replays the child image with only its derived referenced columns fast-forwarded.
fn fast_forward_child(
    violation: &crate::live::ForeignKeyViolation,
    candidate: &DeferredSupersededInsertCandidate,
    plan: &super::derived_fk_fastforward::DerivedFkFastForwardPlan,
) -> Result<super::foreign_key_repair::ForeignKeyRepairProof, String> {
    let change = &candidate.historical_change;
    let row = super::foreign_key_repair::fast_forwarded_child_row(
        &change.writable_columns,
        &change.source_values,
        plan,
    )
    .ok_or_else(|| {
        "superseded foreign key insert rejected: child image could not be rebuilt".to_string()
    })?;
    let statement =
        build_exact_row_insert_statement(&violation.child_schema, &violation.child_table, &row)
            .map_err(|error| format!("superseded foreign key insert rejected: {error}"))?;
    Ok(super::foreign_key_repair::ForeignKeyRepairProof {
        statements: vec![statement],
        evidence: plan.evidence(),
    })
}

/// Converts a source parent row into the exact-insert shape, in the parent's stored column order.
fn snapshot_row_as_source_row(
    row: &crate::snapshot::SnapshotRow,
    parent: &crate::inventory::TableInventory,
) -> super::superseded_source::CanonicalSourceRow {
    let mut columns = Vec::new();
    let mut values = Vec::new();
    for column in &parent.columns {
        if column.generated.is_some() {
            continue;
        }
        if let Some(value) = row.values.get(&column.name) {
            columns.push(column.name.clone());
            values.push(match value {
                Some(value) => Value::Bytes(value.clone().into_bytes()),
                None => Value::NULL,
            });
        }
    }
    super::superseded_source::CanonicalSourceRow {
        columns,
        values,
        // Not a source evidence row; the insert builder reads only columns and values.
        hash: String::new(),
    }
}

fn verify_with_source_loader<E, L>(
    candidate: &DeferredSupersededInsertCandidate,
    xid_end_position: u64,
    source_loader: &mut L,
    target: &E,
) -> Result<SupersededInsertProof, String>
where
    E: TransactionalTargetExecutor,
    L: FnMut(u64, &str) -> Result<SupersededSourceEvidence, String>,
{
    validate_exact_scope(candidate)?;
    let historical = historical_identity(candidate)?;
    let source = source_loader(historical.primary_key, &historical.name)
        .map_err(|error| format!("superseded source evidence failed: {error}"))?;
    let target = if candidate.observation.table == COMICS_TABLE {
        target.read_locked_comics_supersession_evidence(
            &historical.primary_key_value,
            &historical.name_value,
        )
    } else {
        target.read_locked_users_supersession_evidence(
            &historical.primary_key_value,
            &historical.name_value,
        )
    }
    .map_err(|error| format!("superseded target evidence failed: {error}"))?;
    let identity_column = if candidate.observation.table == COMICS_TABLE {
        "slug"
    } else {
        "name"
    };
    let source_sql = super::superseded_source::identity_row_query(
        &source.columns,
        &candidate.observation.table,
        identity_column,
    )
    .map_err(|error| format!("superseded source SQL formatting failed: {error}"))?;
    let target_sql = crate::mysql_client::build_locked_identity_evidence_sql(
        &target.columns,
        &candidate.observation.table,
        identity_column,
    )
    .map_err(|error| format!("superseded target SQL formatting failed: {error}"))?;
    let evidence_params = format!(
        "{{primary_key={},identity={:?}}}",
        historical.primary_key, historical.name
    );
    let input = verification_input(candidate, xid_end_position, &historical, source, target)?;
    verify_superseded_insert(&input).map_err(|rejection| {
        format!(
            "superseded insert rejected: {rejection:?}; evidence_params={evidence_params}; source_sql={source_sql}; target_sql={target_sql}"
        )
    })
}

fn validate_exact_scope(candidate: &DeferredSupersededInsertCandidate) -> Result<(), String> {
    let observation = &candidate.observation;
    let supported_scope = observation.schema == USERS_SCHEMA
        && ((observation.table == USERS_TABLE
            && observation.duplicate_index.as_deref() == Some(USERS_NAME_INDEX))
            || (observation.table == COMICS_TABLE
                && observation.duplicate_index.as_deref() == Some(COMICS_SLUG_INDEX)));
    if !supported_scope {
        return Err("superseded insert rejected: requires globalcomix.users/users.name or globalcomix.comics/comics.slug".to_string());
    }
    if observation.operation != crate::conflict_repair::ConflictOperation::Insert {
        return Err("superseded insert rejected: requires INSERT".to_string());
    }
    if candidate.historical_change.kind != crate::target::TargetRowChangeKind::Insert {
        return Err("superseded insert rejected: historical change must be INSERT".to_string());
    }
    Ok(())
}

struct HistoricalIdentity {
    primary_key: u64,
    name: String,
    primary_key_value: Value,
    name_value: Value,
    image_hash: String,
}

fn historical_identity(
    candidate: &DeferredSupersededInsertCandidate,
) -> Result<HistoricalIdentity, String> {
    let change = &candidate.historical_change;
    if change.writable_columns.len() != change.source_values.len() {
        return Err("historical users change column/value count mismatch".to_string());
    }
    let primary_key_value = value_for_column(change, "id")?.clone();
    let identity_column = if candidate.observation.table == COMICS_TABLE {
        "slug"
    } else {
        "name"
    };
    let name_value = value_for_column(change, identity_column)?.clone();
    let primary_key = value_u64(&primary_key_value, "historical id")?;
    let name = value_string(&name_value, "historical unique identity")?;
    let image_hash = super::superseded_source::hash_canonical_row(
        &change.writable_columns,
        &change.source_values,
    )
    .map_err(|error| format!("historical users image hash failed: {error}"))?;
    Ok(HistoricalIdentity {
        primary_key,
        name,
        primary_key_value,
        name_value,
        image_hash,
    })
}

fn value_for_column<'a>(
    change: &'a crate::target::TargetRowChange,
    column: &str,
) -> Result<&'a Value, String> {
    let index = change
        .writable_columns
        .iter()
        .position(|candidate| candidate == column)
        .ok_or_else(|| format!("complete historical users change is missing {column}"))?;
    change
        .source_values
        .get(index)
        .ok_or_else(|| format!("complete historical users change has no value for {column}"))
}

fn verification_input(
    candidate: &DeferredSupersededInsertCandidate,
    xid_end_position: u64,
    historical: &HistoricalIdentity,
    source: SupersededSourceEvidence,
    target: UsersActiveTransactionEvidence,
) -> Result<SupersededInsertVerificationInput, String> {
    if source.columns != target.columns {
        return Err(format!(
            "source/target users evidence column mismatch: source {:?}, target {:?}",
            source.columns, target.columns
        ));
    }
    let id_index = column_index(&source.columns, "id")?;
    let identity_column = if candidate.observation.table == COMICS_TABLE {
        "slug"
    } else {
        "name"
    };
    let name_index = column_index(&source.columns, identity_column)?;
    let source_rows = classify_source_rows(&source, id_index, name_index, historical)?;
    let target_rows = classify_target_rows(&target, id_index, name_index, historical)?;

    Ok(SupersededInsertVerificationInput {
        schema: candidate.observation.schema.clone(),
        table: candidate.observation.table.clone(),
        operation: crate::conflict_repair::ConflictOperation::Insert,
        duplicate_index: candidate
            .observation
            .duplicate_index
            .clone()
            .unwrap_or_default(),
        candidate_xid: BinlogCoordinate {
            file: candidate.observation.coordinate.file.clone(),
            position: xid_end_position,
        },
        source_snapshot: BinlogCoordinate {
            file: source.snapshot.file,
            position: source.snapshot.position,
        },
        historical_primary_key: historical.primary_key.to_string(),
        historical_name: historical.name.clone(),
        historical_image_hash: historical.image_hash.clone(),
        source_primary_row_count: source_rows.primary_count,
        source_primary_name: source_rows.primary_name,
        source_primary_hash: source_rows.primary_hash,
        source_owner_row_count: source_rows.owner_count,
        source_owner_primary_key: source_rows.owner_primary_key,
        source_owner_hash: source_rows.owner_hash,
        target_rows_read_for_update: true,
        target_primary_row_count: target_rows.primary_count,
        target_primary_hash: target_rows.primary_hash,
        target_owner_row_count: target_rows.owner_count,
        target_owner_primary_key: target_rows.owner_primary_key,
        target_owner_identity: target_rows.owner_identity,
        target_owner_hash: target_rows.owner_hash,
    })
}

#[derive(Default)]
struct ClassifiedRows {
    primary_count: usize,
    primary_name: String,
    primary_hash: String,
    owner_count: usize,
    owner_primary_key: String,
    owner_identity: String,
    owner_hash: String,
}

fn classify_source_rows(
    evidence: &SupersededSourceEvidence,
    id_index: usize,
    name_index: usize,
    historical: &HistoricalIdentity,
) -> Result<ClassifiedRows, String> {
    let rows = evidence
        .matching_rows
        .iter()
        .map(|row| {
            (
                &row.values,
                crate::target::hash_ordered_mysql_row(&row.values),
            )
        })
        .collect::<Vec<_>>();
    classify_rows(&rows, id_index, name_index, historical)
}

fn classify_target_rows(
    evidence: &UsersActiveTransactionEvidence,
    id_index: usize,
    name_index: usize,
    historical: &HistoricalIdentity,
) -> Result<ClassifiedRows, String> {
    let rows = evidence
        .rows
        .iter()
        .map(|row| (&row.values, row.row_hash.clone()))
        .collect::<Vec<_>>();
    classify_rows(&rows, id_index, name_index, historical)
}

fn classify_rows(
    rows: &[(&Vec<Value>, String)],
    id_index: usize,
    name_index: usize,
    historical: &HistoricalIdentity,
) -> Result<ClassifiedRows, String> {
    let mut classified = ClassifiedRows::default();
    for (values, hash) in rows {
        let id = values
            .get(id_index)
            .ok_or_else(|| "users evidence row is missing id".to_string())?;
        let name = values
            .get(name_index)
            .ok_or_else(|| "users evidence row is missing name".to_string())?;
        if value_u64(id, "users evidence id")? == historical.primary_key {
            classified.primary_count += 1;
            classified.primary_name = value_string(name, "users evidence primary name")?;
            classified.primary_hash.clone_from(hash);
        }
        if value_string(name, "users evidence owner name")? == historical.name {
            classified.owner_count += 1;
            classified.owner_primary_key = value_u64(id, "users evidence owner id")?.to_string();
            classified.owner_identity = value_string(name, "users evidence owner name")?;
            classified.owner_hash.clone_from(hash);
        }
    }
    Ok(classified)
}

fn column_index(columns: &[String], column: &str) -> Result<usize, String> {
    columns
        .iter()
        .position(|candidate| candidate == column)
        .ok_or_else(|| format!("users evidence is missing {column} column"))
}

fn value_u64(value: &Value, label: &str) -> Result<u64, String> {
    match value {
        Value::UInt(value) => Ok(*value),
        Value::Int(value) if *value >= 0 => Ok(*value as u64),
        Value::Bytes(value) => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| format!("{label} is not an unsigned integer")),
        _ => Err(format!("{label} is not an unsigned integer")),
    }
}

fn value_string(value: &Value, label: &str) -> Result<String, String> {
    match value {
        Value::Bytes(value) => {
            String::from_utf8(value.clone()).map_err(|_| format!("{label} is not valid UTF-8"))
        }
        _ => Err(format!("{label} is not text")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::{
        LockedUsersRowEvidence, SqlStatement, TargetExecuteError, TargetExecutionOutcome,
        TargetExecutor, TargetRowChange,
    };
    use std::cell::RefCell;

    struct FakeTarget {
        evidence: RefCell<Option<Result<UsersActiveTransactionEvidence, TargetExecuteError>>>,
    }

    impl TargetExecutor for FakeTarget {
        fn execute(&self, _statement: &SqlStatement) -> Result<(), TargetExecuteError> {
            Ok(())
        }

        fn execute_row_change(
            &self,
            _change: &TargetRowChange,
        ) -> Result<TargetExecutionOutcome, TargetExecuteError> {
            Ok(TargetExecutionOutcome::Applied)
        }
    }

    impl TransactionalTargetExecutor for FakeTarget {
        fn begin_transaction(&self) -> Result<(), TargetExecuteError> {
            Ok(())
        }

        fn load_transaction_checkpoint_for_update(
            &self,
            _checkpoint_table: &str,
            _checkpoint_name: &str,
        ) -> Result<Option<crate::checkpoint::Checkpoint>, TargetExecuteError> {
            Ok(None)
        }

        fn save_transaction_checkpoint(
            &self,
            _checkpoint_table: &str,
            _checkpoint_name: &str,
            _checkpoint: &crate::checkpoint::Checkpoint,
        ) -> Result<(), TargetExecuteError> {
            Ok(())
        }

        fn read_locked_users_supersession_evidence(
            &self,
            _historical_primary_key: &Value,
            _historical_name: &Value,
        ) -> Result<UsersActiveTransactionEvidence, TargetExecuteError> {
            self.evidence
                .borrow_mut()
                .take()
                .expect("one target evidence read")
        }

        fn read_locked_comics_supersession_evidence(
            &self,
            _historical_primary_key: &Value,
            _historical_slug: &Value,
        ) -> Result<UsersActiveTransactionEvidence, TargetExecuteError> {
            self.evidence
                .borrow_mut()
                .take()
                .expect("one target evidence read")
        }

        fn commit_transaction(&self) -> Result<(), TargetExecuteError> {
            Ok(())
        }

        fn rollback_transaction(&self) -> Result<(), TargetExecuteError> {
            Ok(())
        }
    }

    fn values(id: u64, name: &str) -> Vec<Value> {
        vec![Value::UInt(id), Value::Bytes(name.as_bytes().to_vec())]
    }

    fn source_evidence() -> SupersededSourceEvidence {
        let columns = vec!["id".to_string(), "name".to_string()];
        let primary = values(2_070_980, "vngt");
        let owner = values(2_071_305, "-3572");
        SupersededSourceEvidence {
            snapshot: super::super::superseded_source::SourceSnapshotCoordinate {
                file: "mysqld-bin.002740".to_string(),
                position: 1_004_163_590,
            },
            columns: columns.clone(),
            matching_rows: vec![
                super::super::superseded_source::CanonicalSourceRow {
                    columns: columns.clone(),
                    hash: super::super::superseded_source::hash_canonical_row(&columns, &primary)
                        .expect("primary hash"),
                    values: primary,
                },
                super::super::superseded_source::CanonicalSourceRow {
                    columns: columns.clone(),
                    hash: super::super::superseded_source::hash_canonical_row(&columns, &owner)
                        .expect("owner hash"),
                    values: owner,
                },
            ],
        }
    }

    fn target_evidence() -> UsersActiveTransactionEvidence {
        let rows = [values(2_070_980, "vngt"), values(2_071_305, "-3572")]
            .into_iter()
            .map(|values| LockedUsersRowEvidence {
                row_hash: crate::target::hash_ordered_mysql_row(&values),
                values,
            })
            .collect();
        UsersActiveTransactionEvidence {
            columns: vec!["id".to_string(), "name".to_string()],
            rows,
        }
    }

    fn candidate() -> DeferredSupersededInsertCandidate {
        DeferredSupersededInsertCandidate {
            observation: crate::conflict_repair::ConflictObservation {
                source_identity: "source".to_string(),
                source_server_id: 3,
                coordinate: crate::conflict_repair::ConflictCoordinate {
                    file: "mysqld-bin.002709".to_string(),
                    start_position: 404_034_840,
                    end_position: 0,
                },
                schema: USERS_SCHEMA.to_string(),
                table: USERS_TABLE.to_string(),
                operation: crate::conflict_repair::ConflictOperation::Insert,
                source_primary_key: vec!["2070980".to_string()],
                duplicate_index: Some(USERS_NAME_INDEX.to_string()),
                duplicate_owner_primary_key: None,
                error_code: 1062,
                error_text: "duplicate".to_string(),
                observed_at_ms: 1,
                parent_recovery: None,
            },
            historical_change: TargetRowChange {
                statement: SqlStatement {
                    sql: "INSERT".to_string(),
                    params: Vec::new(),
                },
                kind: crate::target::TargetRowChangeKind::Insert,
                table: USERS_TABLE.to_string(),
                primary_key_columns: vec!["id".to_string()],
                primary_key_values: vec![Value::UInt(2_070_980)],
                writable_columns: vec!["id".to_string(), "name".to_string()],
                source_values: values(2_070_980, "-3572"),
                set_columns: vec![None, None],
            },
        }
    }

    #[test]
    fn combines_source_snapshot_and_active_target_transaction_into_shared_proof() {
        let target = FakeTarget {
            evidence: RefCell::new(Some(Ok(target_evidence()))),
        };
        let mut source = |_: u64, _: &str| Ok(source_evidence());

        let proof = verify_with_source_loader(&candidate(), 404_038_011, &mut source, &target)
            .expect("superseded proof");

        assert_eq!(proof.source_snapshot.position, 1_004_163_590);
        assert_eq!(proof.source_primary_hash, proof.target_primary_hash);
        assert_eq!(proof.source_owner_hash, proof.target_owner_hash);
    }

    #[test]
    fn maps_source_adapter_failure_precisely() {
        let target = FakeTarget {
            evidence: RefCell::new(Some(Ok(target_evidence()))),
        };
        let mut source = |_: u64, _: &str| Err("source unavailable".to_string());

        let error = verify_with_source_loader(&candidate(), 404_038_011, &mut source, &target)
            .expect_err("source failure");

        assert_eq!(
            error,
            "superseded source evidence failed: source unavailable"
        );
    }

    #[test]
    fn owner_hash_mismatch_reports_parameterized_source_and_target_sql() {
        let mut candidate = candidate();
        candidate.observation.table = COMICS_TABLE.to_string();
        candidate.observation.duplicate_index = Some(COMICS_SLUG_INDEX.to_string());
        candidate.observation.source_primary_key = vec!["48054".to_string()];
        candidate.historical_change.table = "globalcomix.comics".to_string();
        candidate.historical_change.writable_columns = vec!["id".to_string(), "slug".to_string()];
        candidate.historical_change.primary_key_values = vec![Value::UInt(48_054)];
        candidate.historical_change.source_values = values(48_054, "misc");

        let mut source_evidence = source_evidence();
        source_evidence.columns[1] = "slug".to_string();
        for row in &mut source_evidence.matching_rows {
            row.columns[1] = "slug".to_string();
        }
        let mut target_evidence = target_evidence();
        target_evidence.columns[1] = "slug".to_string();
        target_evidence.rows[1].row_hash = "different-owner-hash".to_string();
        let target = FakeTarget {
            evidence: RefCell::new(Some(Ok(target_evidence))),
        };
        let mut source = |_primary_key: u64, _slug: &str| Ok(source_evidence.clone());

        let error = verify_with_source_loader(&candidate, 531_241_781, &mut source, &target)
            .expect_err("owner mismatch must include evidence queries");

        assert!(error.contains("source_sql=SELECT `id`,`slug` FROM `globalcomix`.`comics` WHERE `id` = ? OR `slug` = ? ORDER BY `id`"));
        assert!(error.contains("target_sql=SELECT `id`, `slug` FROM `globalcomix`.`comics` WHERE `id` = ? OR `slug` = ? ORDER BY `id` FOR UPDATE"));
        assert!(error.contains("evidence_params={primary_key=48054,identity=\"misc\"}"));
        assert!(!error.contains("password"));
        assert!(!error.contains("host"));
    }

    #[test]
    fn maps_active_target_adapter_failure_precisely() {
        let target = FakeTarget {
            evidence: RefCell::new(Some(Err(TargetExecuteError::new("lock read failed")))),
        };
        let mut source = |_: u64, _: &str| Ok(source_evidence());

        let error = verify_with_source_loader(&candidate(), 404_038_011, &mut source, &target)
            .expect_err("target failure");

        assert_eq!(error, "superseded target evidence failed: lock read failed");
    }
}
