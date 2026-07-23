use super::superseded_insert::{
    BinlogCoordinate, SupersededInsertProof, SupersededInsertVerificationInput,
    verify_superseded_insert,
};
use super::superseded_source::{SupersededSourceEvidence, load_superseded_source_evidence};
use super::transaction::SupersededInsertVerifier;
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::row::DeferredSupersededInsertCandidate;
use crate::target::{TransactionalTargetExecutor, UsersActiveTransactionEvidence};
use mysql::Value;

const USERS_SCHEMA: &str = "globalcomix";
const USERS_TABLE: &str = "users";
const USERS_NAME_INDEX: &str = "users.name";

pub(crate) struct ProductionSupersededInsertVerifier<'a, E> {
    source: MySqlConnectionConfig,
    target: &'a E,
}

impl<'a, E> ProductionSupersededInsertVerifier<'a, E> {
    pub(crate) fn new(source: &MySqlConnectionConfig, target: &'a E) -> Self {
        Self {
            source: source.clone(),
            target,
        }
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
    ) -> Result<SupersededInsertProof, String> {
        let mut source = |primary_key: u64, name: &str| {
            load_superseded_source_evidence(&self.source, primary_key, name)
                .map_err(|error| error.to_string())
        };
        verify_with_source_loader(candidate, xid_end_position, &mut source, self.target)
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
    let target = target
        .read_locked_users_supersession_evidence(
            &historical.primary_key_value,
            &historical.name_value,
        )
        .map_err(|error| format!("superseded target evidence failed: {error}"))?;
    let input = verification_input(candidate, xid_end_position, &historical, source, target)?;
    verify_superseded_insert(&input)
        .map_err(|rejection| format!("superseded insert rejected: {rejection:?}"))
}

fn validate_exact_scope(candidate: &DeferredSupersededInsertCandidate) -> Result<(), String> {
    let observation = &candidate.observation;
    if observation.schema != USERS_SCHEMA || observation.table != USERS_TABLE {
        return Err("superseded insert verifier requires globalcomix.users".to_string());
    }
    if observation.operation != crate::conflict_repair::ConflictOperation::Insert {
        return Err("superseded insert verifier requires INSERT".to_string());
    }
    if observation.duplicate_index.as_deref() != Some(USERS_NAME_INDEX) {
        return Err("superseded insert verifier requires duplicate index users.name".to_string());
    }
    if candidate.historical_change.kind != crate::target::TargetRowChangeKind::Insert {
        return Err("superseded insert historical change must be INSERT".to_string());
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
    let name_value = value_for_column(change, "name")?.clone();
    let primary_key = value_u64(&primary_key_value, "historical users.id")?;
    let name = value_string(&name_value, "historical users.name")?;
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
    let name_index = column_index(&source.columns, "name")?;
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
