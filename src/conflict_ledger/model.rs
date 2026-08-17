use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum ConflictOperation {
    Insert,
    Update,
    Delete,
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

    pub fn source_row_identity(&self) -> String {
        source_row_identity(
            &self.source_identity,
            &self.schema,
            &self.table,
            &self.source_primary_key,
        )
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
