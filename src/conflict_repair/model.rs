use crate::snapshot::SnapshotRow;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct CanonicalForeignKey {
    pub constraint_schema: String,
    pub constraint_name: String,
    pub child_schema: String,
    pub child_table: String,
    pub child_columns: Vec<String>,
    pub parent_schema: String,
    pub parent_table: String,
    pub parent_columns: Vec<String>,
    pub update_rule: String,
    pub delete_rule: String,
    pub match_option: String,
    pub enforced: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalForeignKeyRow {
    pub constraint_schema: String,
    pub constraint_name: String,
    pub child_schema: String,
    pub child_table: String,
    pub child_column: String,
    pub ordinal_position: u32,
    pub parent_schema: String,
    pub parent_table: String,
    pub parent_column: String,
    pub update_rule: String,
    pub delete_rule: String,
    pub match_option: String,
    pub enforced: bool,
}

pub fn canonicalize_foreign_keys(
    rows: Vec<CanonicalForeignKeyRow>,
) -> Result<Vec<CanonicalForeignKey>, String> {
    group_foreign_key_rows(sort_foreign_key_rows(rows))
        .into_iter()
        .map(build_canonical_foreign_key)
        .collect()
}

fn sort_foreign_key_rows(mut rows: Vec<CanonicalForeignKeyRow>) -> Vec<CanonicalForeignKeyRow> {
    rows.sort_by(|left, right| {
        (
            &left.constraint_schema,
            &left.constraint_name,
            &left.child_schema,
            &left.child_table,
            left.ordinal_position,
        )
            .cmp(&(
                &right.constraint_schema,
                &right.constraint_name,
                &right.child_schema,
                &right.child_table,
                right.ordinal_position,
            ))
    });
    rows
}

fn group_foreign_key_rows(
    rows: Vec<CanonicalForeignKeyRow>,
) -> BTreeMap<(String, String, String, String), Vec<CanonicalForeignKeyRow>> {
    rows.into_iter().fold(BTreeMap::new(), |mut grouped, row| {
        grouped
            .entry((
                row.constraint_schema.clone(),
                row.constraint_name.clone(),
                row.child_schema.clone(),
                row.child_table.clone(),
            ))
            .or_default()
            .push(row);
        grouped
    })
}

type ForeignKeyGroup = (
    (String, String, String, String),
    Vec<CanonicalForeignKeyRow>,
);

fn build_canonical_foreign_key(
    ((constraint_schema, constraint_name, child_schema, child_table), rows): ForeignKeyGroup,
) -> Result<CanonicalForeignKey, String> {
    let first = first_foreign_key_row(&rows)?;
    let update_rule = normalize_fk_rule(&first.update_rule);
    let delete_rule = normalize_fk_rule(&first.delete_rule);
    validate_foreign_key_group(
        &constraint_schema,
        &constraint_name,
        first,
        &rows,
        &update_rule,
        &delete_rule,
    )?;
    Ok(CanonicalForeignKey {
        constraint_schema,
        constraint_name,
        child_schema,
        child_table,
        child_columns: rows.iter().map(|row| row.child_column.clone()).collect(),
        parent_schema: first.parent_schema.clone(),
        parent_table: first.parent_table.clone(),
        parent_columns: rows.iter().map(|row| row.parent_column.clone()).collect(),
        update_rule,
        delete_rule,
        match_option: first.match_option.clone(),
        enforced: first.enforced,
    })
}

fn first_foreign_key_row(
    rows: &[CanonicalForeignKeyRow],
) -> Result<&CanonicalForeignKeyRow, String> {
    rows.first()
        .ok_or_else(|| "empty foreign-key group".to_string())
}

fn validate_foreign_key_group(
    schema: &str,
    name: &str,
    first: &CanonicalForeignKeyRow,
    rows: &[CanonicalForeignKeyRow],
    update_rule: &str,
    delete_rule: &str,
) -> Result<(), String> {
    let consistent = rows.iter().skip(1).all(|row| {
        row.child_schema == first.child_schema
            && row.child_table == first.child_table
            && row.parent_schema == first.parent_schema
            && row.parent_table == first.parent_table
            && normalize_fk_rule(&row.update_rule) == update_rule
            && normalize_fk_rule(&row.delete_rule) == delete_rule
            && row.match_option == first.match_option
            && row.enforced == first.enforced
    });
    consistent
        .then_some(())
        .ok_or_else(|| format!("foreign-key constraint {schema}.{name} has inconsistent metadata"))
}

pub(crate) fn normalize_fk_rule(rule: &str) -> String {
    if rule.eq_ignore_ascii_case("NO ACTION") {
        "RESTRICT".to_string()
    } else {
        rule.to_ascii_uppercase()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum ConflictOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum ConflictStatus {
    Unresolved,
    Resolved,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ConflictCoordinate {
    pub file: String,
    pub start_position: u64,
    pub end_position: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ConflictKey {
    pub source_identity: String,
    pub source_server_id: u64,
    pub coordinate: ConflictCoordinate,
    pub schema: String,
    pub table: String,
    pub operation: ConflictOperation,
    pub source_primary_key: Vec<String>,
}

impl ConflictKey {
    pub fn conflict_identity(&self) -> String {
        conflict_identity(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConflictResolution {
    pub source_identity: String,
    pub schema: String,
    pub table: String,
    pub source_primary_key: Vec<String>,
    pub repair_run_id: String,
    pub evidence: String,
}

impl ConflictResolution {
    pub fn source_row_identity(&self) -> String {
        source_row_identity(
            &self.source_identity,
            &self.schema,
            &self.table,
            &self.source_primary_key,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConflictObservation {
    pub source_identity: String,
    pub source_server_id: u64,
    pub coordinate: ConflictCoordinate,
    pub schema: String,
    pub table: String,
    pub operation: ConflictOperation,
    pub source_primary_key: Vec<String>,
    pub duplicate_index: Option<String>,
    pub duplicate_owner_primary_key: Option<Vec<String>>,
    pub error_code: u16,
    pub error_text: String,
    pub observed_at_ms: u64,
}

impl ConflictObservation {
    pub fn source_row_identity(&self) -> String {
        source_row_identity(
            &self.source_identity,
            &self.schema,
            &self.table,
            &self.source_primary_key,
        )
    }

    pub fn key(&self) -> ConflictKey {
        ConflictKey {
            source_identity: self.source_identity.clone(),
            source_server_id: self.source_server_id,
            coordinate: self.coordinate.clone(),
            schema: self.schema.clone(),
            table: self.table.clone(),
            operation: self.operation,
            source_primary_key: self.source_primary_key.clone(),
        }
    }

    pub fn conflict_identity(&self) -> String {
        self.key().conflict_identity()
    }
}

const CONFLICT_IDENTITY_HEX_LENGTH: usize = 64;

pub fn conflict_identity(key: &ConflictKey) -> String {
    sha256_identity(canonical_conflict_identity_input(key))
}

pub fn source_row_identity(
    source_identity: &str,
    schema: &str,
    table: &str,
    source_primary_key: &[String],
) -> String {
    let source_primary_key_json =
        serde_json::to_vec(source_primary_key).expect("source primary key is serializable");
    let fields: &[&[u8]] = &[
        source_identity.as_bytes(),
        schema.as_bytes(),
        table.as_bytes(),
        &source_primary_key_json,
    ];
    sha256_identity(canonical_length_prefixed_input(fields))
}

fn sha256_identity(input: Vec<u8>) -> String {
    let digest = Sha256::digest(input);
    let mut identity = String::with_capacity(CONFLICT_IDENTITY_HEX_LENGTH);
    for byte in digest {
        use std::fmt::Write;
        write!(&mut identity, "{byte:02x}").expect("writing to a String cannot fail");
    }
    identity
}

pub fn validate_conflict_identity(identity: &str, key: &ConflictKey) -> Result<(), String> {
    validate_sha256_identity_encoding(identity, "conflict identity")?;
    let expected = key.conflict_identity();
    if identity == expected {
        Ok(())
    } else {
        Err(format!(
            "conflict identity mismatch: stored {identity}, expected {expected}"
        ))
    }
}

pub fn validate_source_row_identity(
    identity: &str,
    source_identity: &str,
    schema: &str,
    table: &str,
    source_primary_key: &[String],
) -> Result<(), String> {
    validate_sha256_identity_encoding(identity, "source row identity")?;
    let expected = source_row_identity(source_identity, schema, table, source_primary_key);
    if identity == expected {
        Ok(())
    } else {
        Err(format!(
            "source row identity mismatch: stored {identity}, expected {expected}"
        ))
    }
}

fn validate_sha256_identity_encoding(identity: &str, name: &str) -> Result<(), String> {
    let is_lowercase_hex = identity
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if identity.len() == CONFLICT_IDENTITY_HEX_LENGTH && is_lowercase_hex {
        Ok(())
    } else {
        Err(format!(
            "invalid {name} encoding: expected {CONFLICT_IDENTITY_HEX_LENGTH} lowercase ASCII hex characters"
        ))
    }
}

fn canonical_conflict_identity_input(key: &ConflictKey) -> Vec<u8> {
    let operation = match key.operation {
        ConflictOperation::Insert => "insert",
        ConflictOperation::Update => "update",
        ConflictOperation::Delete => "delete",
    };
    let source_primary_key_json =
        serde_json::to_vec(&key.source_primary_key).expect("source primary key is serializable");
    let fields: &[&[u8]] = &[
        key.source_identity.as_bytes(),
        &key.source_server_id.to_be_bytes(),
        key.coordinate.file.as_bytes(),
        &key.coordinate.start_position.to_be_bytes(),
        key.schema.as_bytes(),
        key.table.as_bytes(),
        operation.as_bytes(),
        &source_primary_key_json,
    ];
    canonical_length_prefixed_input(fields)
}

fn canonical_length_prefixed_input(fields: &[&[u8]]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for field in fields {
        encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
        encoded.extend_from_slice(field);
    }
    encoded
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DuplicateClassification {
    SamePrimary,
    SecondaryUnique { owner_differs: bool },
    Malformed,
}

pub fn classify_duplicate_error(
    error_code: u16,
    error_text: &str,
    source_primary_key: &[String],
    owner_primary_key: Option<&[String]>,
) -> DuplicateClassification {
    if error_code != 1062 {
        return DuplicateClassification::Malformed;
    }
    let Some(key) = duplicate_key_name(error_text) else {
        return DuplicateClassification::Malformed;
    };
    if key.eq_ignore_ascii_case("PRIMARY") {
        return DuplicateClassification::SamePrimary;
    }
    DuplicateClassification::SecondaryUnique {
        owner_differs: owner_primary_key.is_some_and(|owner| owner != source_primary_key),
    }
}

/// Index name from a MySQL `1062` message, e.g. `guests.idx_guest_hash`. Needed as repair evidence
/// because a duplicate owned by another identity is otherwise unreproducible after the run exits.
pub(crate) fn duplicate_key_name(error_text: &str) -> Option<String> {
    let marker = " for key '";
    let start = error_text.find(marker)? + marker.len();
    let remainder = &error_text[start..];
    let end = remainder.find('\'')?;
    let key = &remainder[..end];
    (!key.is_empty()).then(|| key.to_string())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepairInventory {
    pub schema: String,
    pub tables: Vec<String>,
    pub foreign_keys: Vec<CanonicalForeignKey>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepairPlanError {
    SchemaMismatch(String),
    CrossSchema(String),
    Cycle(Vec<String>),
}

impl fmt::Display for RepairPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaMismatch(message) | Self::CrossSchema(message) => {
                formatter.write_str(message)
            }
            Self::Cycle(tables) => write!(
                formatter,
                "foreign-key cycle blocks repair: {}",
                tables.join(" -> ")
            ),
        }
    }
}

impl std::error::Error for RepairPlanError {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepairPlan {
    pub run_id: String,
    pub source_identity: String,
    pub target_identity: String,
    pub inventory_hash: String,
    pub plan_hash: String,
    pub tables: Vec<String>,
    pub delete_order: Vec<String>,
    pub insert_order: Vec<String>,
    pub update_order: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairInput {
    pub source_rows: BTreeMap<String, Vec<SnapshotRow>>,
    pub target_rows: BTreeMap<String, Vec<SnapshotRow>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum RepairPhase {
    Preflight,
    DeleteExtras,
    InsertMissing,
    UpdateDivergent,
    Verify,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepairOperation {
    Delete {
        table: String,
        primary_key: Vec<String>,
    },
    Insert {
        table: String,
        row: SnapshotRow,
    },
    Update {
        table: String,
        row: SnapshotRow,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct RepairOperationKey {
    pub phase: RepairPhase,
    pub table: String,
    pub primary_key: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepairRunState {
    pub run_id: String,
    pub plan_hash: String,
    pub phase: RepairPhase,
    pub completed_operations: BTreeSet<RepairOperationKey>,
    pub status: String,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairReport {
    pub phase: RepairPhase,
    pub actionable_mismatches: usize,
    pub deletes: usize,
    pub inserts: usize,
    pub updates: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RowConflictRecord {
    pub key: ConflictKey,
    pub duplicate_index: Option<String>,
    pub duplicate_owner_primary_key: Option<Vec<String>>,
    pub error_code: u16,
    pub error_text: String,
    pub first_observed_at_ms: u64,
    pub last_observed_at_ms: u64,
    pub attempt_count: u64,
    pub status: ConflictStatus,
    pub repair_run_id: Option<String>,
    pub resolution_evidence: Option<String>,
}
