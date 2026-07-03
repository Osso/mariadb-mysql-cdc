use crate::inventory::ColumnInventory;
use crate::mysql_support::{quote_ident, quote_sql_literal};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChecksumColumn {
    pub name: String,
    pub data_type: String,
    pub column_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChecksumRequest {
    pub table: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<ChecksumColumn>,
    pub start_after: Option<Vec<String>>,
    pub end_at: Option<Vec<String>>,
}

pub fn build_chunk_checksum_sql(request: &ChecksumRequest) -> Result<String, String> {
    validate_bound_arity(
        &request.primary_key,
        request.start_after.as_ref(),
        "start_after",
    )?;
    validate_bound_arity(&request.primary_key, request.end_at.as_ref(), "end_at")?;
    let row_expr = checksum_row_expr(&request.columns)?;
    let bounds = checksum_bounds(request);
    Ok(format!(
        "SELECT COUNT(*) AS row_count, COALESCE(BIT_XOR(CAST(CONV(SUBSTRING(MD5({row_expr}), 1, 16), 16, 10) AS UNSIGNED)), 0) AS checksum FROM {}{bounds}",
        quote_ident(&request.table)
    ))
}

fn checksum_row_expr(columns: &[ChecksumColumn]) -> Result<String, String> {
    let encoded_columns = columns
        .iter()
        .map(encoded_column_expr)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("CONCAT({})", encoded_columns.join(", ")))
}

fn encoded_column_expr(column: &ChecksumColumn) -> Result<String, String> {
    let value = normalized_value_expr(column)?;
    let bytes = byte_value_expr(&value);
    Ok(format!(
        "CASE WHEN {} IS NULL THEN 'N' ELSE CONCAT(OCTET_LENGTH({bytes}), ':', {bytes}) END",
        quote_ident(&column.name)
    ))
}

// FLOAT/DOUBLE string conversion differs between MariaDB and MySQL 8; JSON is
// stored as text on MariaDB but binary-normalized (reformatted) on MySQL 8.
// Both would mismatch on identical data, so they cannot be checksummed.
pub fn is_supported_checksum_type(data_type: &str) -> bool {
    !matches!(
        data_type.to_ascii_lowercase().as_str(),
        "float" | "double" | "real" | "json"
    )
}

fn normalized_value_expr(column: &ChecksumColumn) -> Result<String, String> {
    let quoted = quote_ident(&column.name);
    match column.data_type.to_ascii_lowercase().as_str() {
        "float" | "double" | "real" | "json" => Err(format!(
            "column `{}` uses unsupported checksum type {}",
            column.name, column.data_type
        )),
        "timestamp" => Ok(format!("UNIX_TIMESTAMP({quoted})")),
        "decimal" | "numeric" => Ok(format!("CAST({quoted} AS {})", column.column_type)),
        data_type if is_byte_stable_type(data_type) => Ok(quoted),
        _ => Ok(format!("CAST({quoted} AS CHAR)")),
    }
}

fn byte_value_expr(value: &str) -> String {
    format!("CONVERT({value} USING binary)")
}

fn is_byte_stable_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "char"
            | "varchar"
            | "tinytext"
            | "text"
            | "mediumtext"
            | "longtext"
            | "binary"
            | "varbinary"
            | "tinyblob"
            | "blob"
            | "mediumblob"
            | "longblob"
            | "enum"
            | "set"
    )
}

fn validate_bound_arity(
    primary_key: &[String],
    values: Option<&Vec<String>>,
    label: &str,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    if values.len() != primary_key.len() {
        return Err(format!(
            "{label} has {} values for {} primary-key columns",
            values.len(),
            primary_key.len()
        ));
    }
    Ok(())
}

fn checksum_bounds(request: &ChecksumRequest) -> String {
    let predicates = checksum_bound_predicates(request);
    if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    }
}

fn checksum_bound_predicates(request: &ChecksumRequest) -> Vec<String> {
    let mut predicates = Vec::new();
    if let Some(start_after) = &request.start_after {
        predicates.push(primary_key_bound_predicate(
            &request.primary_key,
            start_after,
            ">",
        ));
    }
    if let Some(end_at) = &request.end_at {
        predicates.push(format!(
            "NOT ({})",
            primary_key_bound_predicate(&request.primary_key, end_at, ">")
        ));
    }
    predicates
}

fn primary_key_bound_predicate(columns: &[String], values: &[String], operator: &str) -> String {
    columns
        .iter()
        .enumerate()
        .map(|(index, _column)| primary_key_bound_branch(columns, values, index, operator))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn primary_key_bound_branch(
    columns: &[String],
    values: &[String],
    index: usize,
    operator: &str,
) -> String {
    let mut parts = Vec::new();
    for equal_index in 0..index {
        parts.push(format!(
            "{} = {}",
            quote_ident(&columns[equal_index]),
            quote_sql_literal(&values[equal_index])
        ));
    }
    parts.push(format!(
        "{} {operator} {}",
        quote_ident(&columns[index]),
        quote_sql_literal(&values[index])
    ));
    format!("({})", parts.join(" AND "))
}

impl From<&ColumnInventory> for ChecksumColumn {
    fn from(column: &ColumnInventory) -> Self {
        Self {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            column_type: column.column_type.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_byte_domain_length_prefixed_checksum_sql() {
        let sql = build_chunk_checksum_sql(&ChecksumRequest {
            table: "orders".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![
                column("id", "bigint", "bigint(20) unsigned"),
                column("slug", "varchar", "varchar(128)"),
                column("price", "decimal", "decimal(10,2)"),
                column("updated_at", "timestamp", "timestamp"),
            ],
            start_after: Some(vec!["10".to_string()]),
            end_at: Some(vec!["20".to_string()]),
        })
        .expect("checksum sql");

        assert!(sql.contains("COUNT(*) AS row_count"));
        assert!(sql.contains("BIT_XOR(CAST(CONV(SUBSTRING(MD5(CONCAT("));
        assert!(sql.contains("OCTET_LENGTH(CONVERT(`slug` USING binary))"));
        assert!(sql.contains("CAST(`price` AS decimal(10,2))"));
        assert!(sql.contains("UNIX_TIMESTAMP(`updated_at`)"));
        assert!(sql.contains("WHERE (`id` > '10') AND NOT ((`id` > '20'))"));
    }

    #[test]
    fn rejects_bounds_that_do_not_match_primary_key_arity() {
        let error = build_chunk_checksum_sql(&ChecksumRequest {
            table: "accounts".to_string(),
            primary_key: vec!["tenant_id".to_string(), "id".to_string()],
            columns: vec![column("id", "bigint", "bigint(20)")],
            start_after: Some(vec!["10".to_string()]),
            end_at: None,
        })
        .expect_err("bad arity");

        assert_eq!(error, "start_after has 1 values for 2 primary-key columns");
    }

    #[test]
    fn rejects_float_columns_for_checksum() {
        let error = build_chunk_checksum_sql(&ChecksumRequest {
            table: "readings".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![column("score", "double", "double")],
            start_after: None,
            end_at: None,
        })
        .expect_err("float unsupported");

        assert_eq!(
            error,
            "column `score` uses unsupported checksum type double"
        );
    }

    fn column(name: &str, data_type: &str, column_type: &str) -> ChecksumColumn {
        ChecksumColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
            column_type: column_type.to_string(),
        }
    }
}
