use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::mysql_support::apply_default_mysql_network_bounds;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, Params, Row, Value};
use sha2::{Digest, Sha256};
use std::fmt;

const USERS_SCHEMA: &str = "globalcomix";
const USERS_TABLE: &str = "users";
const HASH_DOMAIN: &[u8] = b"mariadb-mysql-cdc:superseded-source-row:v1\0";
const COLUMN_QUERY: &str = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SourceSnapshotCoordinate {
    pub(crate) file: String,
    pub(crate) position: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CanonicalSourceRow {
    pub(crate) columns: Vec<String>,
    pub(crate) values: Vec<Value>,
    pub(crate) hash: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SupersededSourceEvidence {
    pub(crate) snapshot: SourceSnapshotCoordinate,
    pub(crate) columns: Vec<String>,
    pub(crate) matching_rows: Vec<CanonicalSourceRow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourceEvidenceError {
    message: String,
}

impl SourceEvidenceError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SourceEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for SourceEvidenceError {}

pub(crate) trait SupersededSourceQuery {
    fn execute(&mut self, sql: &str) -> Result<(), SourceEvidenceError>;
    fn query(&mut self, sql: &str, params: Vec<Value>) -> Result<QueryRows, SourceEvidenceError>;
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryRows {
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<Value>>,
}

pub(crate) struct MySqlSupersededSourceQuery {
    connection: Conn,
}

impl MySqlSupersededSourceQuery {
    pub(crate) fn connect(config: &MySqlConnectionConfig) -> Result<Self, SourceEvidenceError> {
        let options = source_options(config)?;
        Conn::new(options)
            .map(|connection| Self { connection })
            .map_err(|error| {
                SourceEvidenceError::new(format!("failed to connect to source mysql: {error}"))
            })
    }
}

impl SupersededSourceQuery for MySqlSupersededSourceQuery {
    fn execute(&mut self, sql: &str) -> Result<(), SourceEvidenceError> {
        self.connection.query_drop(sql).map_err(|error| {
            SourceEvidenceError::new(format!("source mysql query failed: {error}"))
        })
    }

    fn query(&mut self, sql: &str, params: Vec<Value>) -> Result<QueryRows, SourceEvidenceError> {
        let rows = self
            .connection
            .exec::<Row, _, _>(sql, Params::Positional(params))
            .map_err(|error| {
                SourceEvidenceError::new(format!("source mysql query failed: {error}"))
            })?;
        mysql_rows(rows)
    }
}

pub(crate) fn load_superseded_source_evidence(
    config: &MySqlConnectionConfig,
    historical_primary_key: u64,
    historical_name: &str,
) -> Result<SupersededSourceEvidence, SourceEvidenceError> {
    let mut source = MySqlSupersededSourceQuery::connect(config)?;
    load_superseded_source_evidence_with_query(&mut source, historical_primary_key, historical_name)
}

pub(crate) fn load_superseded_source_evidence_with_query(
    source: &mut impl SupersededSourceQuery,
    historical_primary_key: u64,
    historical_name: &str,
) -> Result<SupersededSourceEvidence, SourceEvidenceError> {
    source.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")?;
    let result = load_evidence_in_transaction(source, historical_primary_key, historical_name);
    finish_transaction(source, result)
}

fn finish_transaction<T>(
    source: &mut impl SupersededSourceQuery,
    result: Result<T, SourceEvidenceError>,
) -> Result<T, SourceEvidenceError> {
    match result {
        Ok(value) => {
            source.execute("COMMIT")?;
            Ok(value)
        }
        Err(error) => match source.execute("ROLLBACK") {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(SourceEvidenceError::new(format!(
                "{error}; rollback failed: {rollback_error}"
            ))),
        },
    }
}

fn load_evidence_in_transaction(
    source: &mut impl SupersededSourceQuery,
    historical_primary_key: u64,
    historical_name: &str,
) -> Result<SupersededSourceEvidence, SourceEvidenceError> {
    let snapshot = load_master_status(source)?;
    let columns = load_users_columns(source)?;
    let row_query = users_row_query(&columns)?;
    let result = source.query(
        &row_query,
        vec![
            Value::UInt(historical_primary_key),
            Value::Bytes(historical_name.as_bytes().to_vec()),
        ],
    )?;
    if result.columns != columns {
        return Err(SourceEvidenceError::new(format!(
            "source users result column order mismatch: expected {:?}, got {:?}",
            columns, result.columns
        )));
    }
    let matching_rows = result
        .rows
        .into_iter()
        .map(|values| canonical_source_row(&columns, values))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SupersededSourceEvidence {
        snapshot,
        columns,
        matching_rows,
    })
}

fn load_master_status(
    source: &mut impl SupersededSourceQuery,
) -> Result<SourceSnapshotCoordinate, SourceEvidenceError> {
    let result = source.query("SHOW MASTER STATUS", Vec::new())?;
    parse_master_status(&result)
}

fn parse_master_status(
    result: &QueryRows,
) -> Result<SourceSnapshotCoordinate, SourceEvidenceError> {
    if result.rows.len() != 1 {
        return Err(SourceEvidenceError::new(format!(
            "SHOW MASTER STATUS returned {} rows",
            result.rows.len()
        )));
    }
    let row = &result.rows[0];
    let file = value_bytes(row.first(), "SHOW MASTER STATUS binlog file")?;
    let position = value_u64(row.get(1), "SHOW MASTER STATUS position")?;
    if file.is_empty() {
        return Err(SourceEvidenceError::new(
            "SHOW MASTER STATUS binlog file was empty",
        ));
    }
    Ok(SourceSnapshotCoordinate { file, position })
}

fn load_users_columns(
    source: &mut impl SupersededSourceQuery,
) -> Result<Vec<String>, SourceEvidenceError> {
    let result = source.query(
        COLUMN_QUERY,
        vec![
            Value::Bytes(USERS_SCHEMA.as_bytes().to_vec()),
            Value::Bytes(USERS_TABLE.as_bytes().to_vec()),
        ],
    )?;
    let columns = result
        .rows
        .iter()
        .map(|row| value_bytes(row.first(), "users column name"))
        .collect::<Result<Vec<_>, _>>()?;
    if columns.is_empty() {
        return Err(SourceEvidenceError::new(
            "source metadata returned no globalcomix.users columns",
        ));
    }
    if columns.iter().any(|column| !valid_identifier(column)) {
        return Err(SourceEvidenceError::new(
            "source metadata returned an invalid users column identifier",
        ));
    }
    Ok(columns)
}

fn users_row_query(columns: &[String]) -> Result<String, SourceEvidenceError> {
    if columns.is_empty() || columns.iter().any(|column| !valid_identifier(column)) {
        return Err(SourceEvidenceError::new(
            "cannot build users evidence query from invalid columns",
        ));
    }
    let selected = columns
        .iter()
        .map(|column| format!("`{column}`"))
        .collect::<Vec<_>>()
        .join(",");
    Ok(format!(
        "SELECT {selected} FROM `globalcomix`.`users` WHERE `id` = ? OR `name` = ? ORDER BY `id`"
    ))
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn mysql_rows(rows: Vec<Row>) -> Result<QueryRows, SourceEvidenceError> {
    let columns = rows
        .first()
        .map(|row| {
            row.columns_ref()
                .iter()
                .map(|column| column.name_str().into_owned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let rows = rows.into_iter().map(|row| row.unwrap()).collect::<Vec<_>>();
    Ok(QueryRows { columns, rows })
}

fn canonical_source_row(
    columns: &[String],
    values: Vec<Value>,
) -> Result<CanonicalSourceRow, SourceEvidenceError> {
    if columns.len() != values.len() {
        return Err(SourceEvidenceError::new(format!(
            "source users row has {} values for {} columns",
            values.len(),
            columns.len()
        )));
    }
    let hash = hash_canonical_row(columns, &values)?;
    Ok(CanonicalSourceRow {
        columns: columns.to_vec(),
        values,
        hash,
    })
}

pub(crate) fn hash_canonical_row(
    columns: &[String],
    values: &[Value],
) -> Result<String, SourceEvidenceError> {
    if columns.len() != values.len() {
        return Err(SourceEvidenceError::new(
            "canonical row column/value count mismatch",
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(HASH_DOMAIN);
    hash_length(&mut hasher, columns.len());
    for (column, value) in columns.iter().zip(values) {
        hash_bytes(&mut hasher, column.as_bytes());
        hash_value(&mut hasher, value);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn hash_value(hasher: &mut Sha256, value: &Value) {
    match value {
        Value::NULL => hasher.update([0]),
        Value::Bytes(bytes) => {
            hasher.update([1]);
            hash_bytes(hasher, bytes);
        }
        Value::Int(value) => {
            hasher.update([2]);
            hasher.update(value.to_be_bytes());
        }
        Value::UInt(value) => {
            hasher.update([3]);
            hasher.update(value.to_be_bytes());
        }
        Value::Float(value) => {
            hasher.update([4]);
            hasher.update(value.to_bits().to_be_bytes());
        }
        Value::Double(value) => {
            hasher.update([5]);
            hasher.update(value.to_bits().to_be_bytes());
        }
        Value::Date(year, month, day, hour, minute, second, micros) => {
            hasher.update([6]);
            hasher.update(year.to_be_bytes());
            hasher.update([*month, *day, *hour, *minute, *second]);
            hasher.update(micros.to_be_bytes());
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            hasher.update([7, u8::from(*negative)]);
            hasher.update(days.to_be_bytes());
            hasher.update([*hours, *minutes, *seconds]);
            hasher.update(micros.to_be_bytes());
        }
    }
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hash_length(hasher, bytes.len());
    hasher.update(bytes);
}

fn hash_length(hasher: &mut Sha256, length: usize) {
    hasher.update((length as u64).to_be_bytes());
}

fn value_bytes(value: Option<&Value>, field: &str) -> Result<String, SourceEvidenceError> {
    match value {
        Some(Value::Bytes(bytes)) => String::from_utf8(bytes.clone())
            .map_err(|_| SourceEvidenceError::new(format!("{field} was not UTF-8"))),
        _ => Err(SourceEvidenceError::new(format!(
            "{field} was missing or not text"
        ))),
    }
}

fn value_u64(value: Option<&Value>, field: &str) -> Result<u64, SourceEvidenceError> {
    match value {
        Some(Value::UInt(value)) => Ok(*value),
        Some(Value::Int(value)) if *value >= 0 => Ok(*value as u64),
        Some(Value::Bytes(bytes)) => std::str::from_utf8(bytes)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| SourceEvidenceError::new(format!("{field} was not numeric"))),
        _ => Err(SourceEvidenceError::new(format!(
            "{field} was missing or not numeric"
        ))),
    }
}

fn source_options(config: &MySqlConnectionConfig) -> Result<Opts, SourceEvidenceError> {
    let builder = OptsBuilder::default()
        .ip_or_hostname(Some(&config.host))
        .tcp_port(config.port)
        .user(Some(&config.user))
        .pass(Some(&config.password))
        .db_name(Some(&config.database))
        .prefer_socket(false);
    Ok(Opts::from(apply_default_mysql_network_bounds(builder)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[derive(Clone, Debug, PartialEq)]
    struct ExpectedQuery {
        sql: String,
        params: Vec<Value>,
        result: Result<QueryRows, SourceEvidenceError>,
    }

    #[derive(Default)]
    struct FakeQuery {
        expected_queries: VecDeque<ExpectedQuery>,
        executed: Vec<String>,
    }

    impl SupersededSourceQuery for FakeQuery {
        fn execute(&mut self, sql: &str) -> Result<(), SourceEvidenceError> {
            self.executed.push(sql.to_string());
            Ok(())
        }

        fn query(
            &mut self,
            sql: &str,
            params: Vec<Value>,
        ) -> Result<QueryRows, SourceEvidenceError> {
            let expected = self.expected_queries.pop_front().expect("unexpected query");
            assert_eq!(sql, expected.sql);
            assert_eq!(params, expected.params);
            expected.result
        }
    }

    fn query_rows(columns: &[&str], rows: Vec<Vec<Value>>) -> QueryRows {
        QueryRows {
            columns: columns.iter().map(|value| value.to_string()).collect(),
            rows,
        }
    }

    fn master_status() -> ExpectedQuery {
        ExpectedQuery {
            sql: "SHOW MASTER STATUS".to_string(),
            params: Vec::new(),
            result: Ok(query_rows(
                &["File", "Position"],
                vec![vec![
                    Value::Bytes(b"mysqld-bin.002740".to_vec()),
                    Value::UInt(1_004_163_590),
                ]],
            )),
        }
    }

    fn metadata(columns: &[&str]) -> ExpectedQuery {
        ExpectedQuery {
            sql: COLUMN_QUERY.to_string(),
            params: vec![
                Value::Bytes(b"globalcomix".to_vec()),
                Value::Bytes(b"users".to_vec()),
            ],
            result: Ok(query_rows(
                &["COLUMN_NAME"],
                columns
                    .iter()
                    .map(|column| vec![Value::Bytes(column.as_bytes().to_vec())])
                    .collect(),
            )),
        }
    }

    #[test]
    fn parses_master_status_coordinate_and_rejects_invalid_shapes() {
        let valid = query_rows(
            &["File", "Position"],
            vec![vec![Value::Bytes(b"mysql-bin.7".to_vec()), Value::Int(42)]],
        );
        assert_eq!(
            parse_master_status(&valid).expect("coordinate"),
            SourceSnapshotCoordinate {
                file: "mysql-bin.7".to_string(),
                position: 42,
            }
        );
        for invalid in [
            query_rows(&["File", "Position"], Vec::new()),
            query_rows(
                &["File", "Position"],
                vec![vec![Value::Bytes(Vec::new()), Value::UInt(1)]],
            ),
            query_rows(
                &["File", "Position"],
                vec![vec![Value::Bytes(b"mysql-bin.7".to_vec()), Value::Int(-1)]],
            ),
        ] {
            assert!(parse_master_status(&invalid).is_err());
        }
    }

    #[test]
    fn canonical_hash_preserves_null_and_mysql_type_boundaries() {
        let columns = vec!["value".to_string()];
        let values = [
            Value::NULL,
            Value::Bytes(b"1".to_vec()),
            Value::Int(1),
            Value::UInt(1),
            Value::Float(1.0),
            Value::Double(1.0),
            Value::Date(2026, 7, 23, 1, 2, 3, 4),
            Value::Time(false, 0, 1, 2, 3, 4),
        ];
        let hashes = values
            .iter()
            .map(|value| hash_canonical_row(&columns, std::slice::from_ref(value)).expect("hash"))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(hashes.len(), values.len());
        assert_eq!(
            hash_canonical_row(&columns, &[Value::NULL]).expect("stable hash"),
            hash_canonical_row(&columns, &[Value::NULL]).expect("stable hash")
        );
        assert_ne!(
            hash_canonical_row(&["a".to_string()], &[Value::Int(1)]).expect("hash"),
            hash_canonical_row(&["b".to_string()], &[Value::Int(1)]).expect("hash")
        );
    }

    #[test]
    fn returns_all_missing_or_ambiguous_matches_for_pure_predicate_counts() {
        for rows in [
            Vec::new(),
            vec![
                vec![Value::UInt(2_070_980), Value::Bytes(b"vngt".to_vec())],
                vec![Value::UInt(2_071_305), Value::Bytes(b"-3572".to_vec())],
                vec![Value::UInt(2_071_306), Value::Bytes(b"-3572".to_vec())],
            ],
        ] {
            let columns = ["id", "name"];
            let mut fake = FakeQuery {
                expected_queries: VecDeque::from([
                    master_status(),
                    metadata(&columns),
                    ExpectedQuery {
                        sql: users_row_query(
                            &columns
                                .iter()
                                .map(|value| value.to_string())
                                .collect::<Vec<_>>(),
                        )
                        .expect("query"),
                        params: vec![Value::UInt(2_070_980), Value::Bytes(b"-3572".to_vec())],
                        result: Ok(query_rows(&columns, rows.clone())),
                    },
                ]),
                executed: Vec::new(),
            };
            let evidence =
                load_superseded_source_evidence_with_query(&mut fake, 2_070_980, "-3572")
                    .expect("evidence");
            assert_eq!(evidence.matching_rows.len(), rows.len());
            assert_eq!(
                fake.executed,
                ["START TRANSACTION WITH CONSISTENT SNAPSHOT", "COMMIT"]
            );
        }
    }

    #[test]
    fn query_is_exactly_scoped_parameterized_and_preserves_metadata_order() {
        let columns = ["name", "id", "email"];
        let ordered_columns = columns
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let mut fake = FakeQuery {
            expected_queries: VecDeque::from([
                master_status(),
                metadata(&columns),
                ExpectedQuery {
                    sql: "SELECT `name`,`id`,`email` FROM `globalcomix`.`users` WHERE `id` = ? OR `name` = ? ORDER BY `id`".to_string(),
                    params: vec![
                        Value::UInt(2_070_980),
                        Value::Bytes(b"-3572".to_vec()),
                    ],
                    result: Ok(query_rows(
                        &columns,
                        vec![vec![
                            Value::Bytes(b"vngt".to_vec()),
                            Value::UInt(2_070_980),
                            Value::Bytes(b"user@example.com".to_vec()),
                        ]],
                    )),
                },
            ]),
            executed: Vec::new(),
        };

        let evidence = load_superseded_source_evidence_with_query(&mut fake, 2_070_980, "-3572")
            .expect("evidence");

        assert_eq!(evidence.columns, ordered_columns);
        assert_eq!(evidence.matching_rows[0].columns, ordered_columns);
        assert!(fake.expected_queries.is_empty());
        assert_eq!(
            fake.executed,
            ["START TRANSACTION WITH CONSISTENT SNAPSHOT", "COMMIT"]
        );
    }

    #[test]
    fn query_failure_rolls_back_explicitly() {
        let mut fake = FakeQuery {
            expected_queries: VecDeque::from([ExpectedQuery {
                sql: "SHOW MASTER STATUS".to_string(),
                params: Vec::new(),
                result: Err(SourceEvidenceError::new("status failed")),
            }]),
            executed: Vec::new(),
        };

        let error = load_superseded_source_evidence_with_query(&mut fake, 2_070_980, "-3572")
            .expect_err("query failure");

        assert_eq!(error.to_string(), "status failed");
        assert_eq!(
            fake.executed,
            ["START TRANSACTION WITH CONSISTENT SNAPSHOT", "ROLLBACK"]
        );
    }
}
