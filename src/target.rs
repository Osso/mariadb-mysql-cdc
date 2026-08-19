use crate::checkpoint::Checkpoint;
use mysql::Value;
use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub struct SqlStatement {
    pub sql: String,
    pub params: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetRowChangeKind {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TargetRowChange {
    pub statement: SqlStatement,
    pub kind: TargetRowChangeKind,
    pub schema: String,
    pub table: String,
    pub values: BTreeMap<String, Value>,
}

pub trait TargetExecutor {
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError>;

    fn execute_row_change(&self, change: &TargetRowChange) -> Result<(), TargetExecuteError> {
        self.execute(&change.statement)
    }
}

pub trait TransactionalTargetExecutor: TargetExecutor {
    fn acquire_stream_lease(&self, _lease_name: &str) -> Result<(), TargetExecuteError> {
        Ok(())
    }
    fn begin_transaction(&self) -> Result<(), TargetExecuteError>;
    fn load_transaction_checkpoint_for_update(
        &self,
        checkpoint_table: &str,
        checkpoint_name: &str,
    ) -> Result<Option<Checkpoint>, TargetExecuteError>;
    fn save_transaction_checkpoint(
        &self,
        checkpoint_table: &str,
        checkpoint_name: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), TargetExecuteError>;
    fn commit_transaction(&self) -> Result<(), TargetExecuteError>;
    fn rollback_transaction(&self) -> Result<(), TargetExecuteError>;
}

impl<E> TransactionalTargetExecutor for &E
where
    E: TransactionalTargetExecutor,
{
    fn acquire_stream_lease(&self, lease_name: &str) -> Result<(), TargetExecuteError> {
        (*self).acquire_stream_lease(lease_name)
    }

    fn begin_transaction(&self) -> Result<(), TargetExecuteError> {
        (*self).begin_transaction()
    }

    fn load_transaction_checkpoint_for_update(
        &self,
        checkpoint_table: &str,
        checkpoint_name: &str,
    ) -> Result<Option<Checkpoint>, TargetExecuteError> {
        (*self).load_transaction_checkpoint_for_update(checkpoint_table, checkpoint_name)
    }

    fn save_transaction_checkpoint(
        &self,
        checkpoint_table: &str,
        checkpoint_name: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), TargetExecuteError> {
        (*self).save_transaction_checkpoint(checkpoint_table, checkpoint_name, checkpoint)
    }

    fn commit_transaction(&self) -> Result<(), TargetExecuteError> {
        (*self).commit_transaction()
    }

    fn rollback_transaction(&self) -> Result<(), TargetExecuteError> {
        (*self).rollback_transaction()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetExecuteError {
    message: String,
    mysql_code: Option<u16>,
}

impl TargetExecuteError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            mysql_code: None,
        }
    }

    pub fn from_mysql(code: u16, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            mysql_code: Some(code),
        }
    }

    pub fn mysql_code(&self) -> Option<u16> {
        self.mysql_code
    }
}

impl fmt::Display for TargetExecuteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for TargetExecuteError {}

pub fn duplicate_index_from_error(error_text: &str) -> Option<String> {
    let marker = " for key '";
    let start = error_text.find(marker)? + marker.len();
    let remainder = &error_text[start..];
    let end = remainder.find('\'')?;
    let key = &remainder[..end];
    (!key.is_empty()).then(|| key.to_string())
}

pub fn render_sql_statement(statement: &SqlStatement) -> Result<String, TargetExecuteError> {
    render_statement_params(statement, |value| Ok(quote_sql_value(value)))
}

pub(crate) fn render_submitted_sql_statement(
    statement: &SqlStatement,
) -> Result<String, TargetExecuteError> {
    render_statement_params(statement, quote_submitted_sql_value)
}

fn render_statement_params(
    statement: &SqlStatement,
    quote: impl Fn(&Value) -> Result<String, TargetExecuteError>,
) -> Result<String, TargetExecuteError> {
    if statement.params.is_empty() {
        return Ok(statement.sql.clone());
    }

    let placeholder_count = statement.sql.matches('?').count();
    if placeholder_count != statement.params.len() {
        return Err(TargetExecuteError::new(format!(
            "statement has {placeholder_count} placeholders and {} params",
            statement.params.len()
        )));
    }

    let mut rendered = String::new();
    let mut params = statement.params.iter();
    for segment in statement.sql.split('?') {
        rendered.push_str(segment);
        if let Some(param) = params.next() {
            rendered.push_str(&quote(param)?);
        }
    }

    Ok(rendered)
}

fn quote_sql_value(value: &Value) -> String {
    match value {
        Value::NULL => "NULL".to_string(),
        Value::Bytes(bytes) => quote_sql_literal(&String::from_utf8_lossy(bytes)),
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::Date(year, month, day, hour, minute, second, micros) => quote_sql_literal(&format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"
        )),
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let sign = if *negative { "-" } else { "" };
            let hours = u32::from(*hours) + (*days * 24);
            quote_sql_literal(&format!(
                "{sign}{hours:02}:{minutes:02}:{seconds:02}.{micros:06}"
            ))
        }
    }
}

fn quote_submitted_sql_value(value: &Value) -> Result<String, TargetExecuteError> {
    match value {
        Value::NULL => Ok("NULL".to_string()),
        Value::Bytes(bytes) => Ok(binary_string_sql_literal(bytes)),
        Value::Int(value) => Ok(value.to_string()),
        Value::UInt(value) => Ok(value.to_string()),
        Value::Float(value) if value.is_finite() => Ok(value.to_string()),
        Value::Float(_) => Err(TargetExecuteError::new(
            "submitted SQL cannot encode a non-finite FLOAT",
        )),
        Value::Double(value) if value.is_finite() => Ok(value.to_string()),
        Value::Double(_) => Err(TargetExecuteError::new(
            "submitted SQL cannot encode a non-finite DOUBLE",
        )),
        Value::Date(year, month, day, hour, minute, second, micros) => Ok(quote_sql_literal(
            &format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"),
        )),
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let sign = if *negative { "-" } else { "" };
            let hours = u32::from(*hours) + (*days * 24);
            Ok(quote_sql_literal(&format!(
                "{sign}{hours:02}:{minutes:02}:{seconds:02}.{micros:06}"
            )))
        }
    }
}

fn binary_string_sql_literal(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut literal = String::with_capacity(bytes.len() * 2 + 11);
    literal.push_str("_binary X'");
    for byte in bytes {
        write!(literal, "{byte:02x}").expect("writing hexadecimal bytes to String cannot fail");
    }
    literal.push('\'');
    literal
}

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_sql_statement_params_as_literals() {
        let rendered = render_sql_statement(&SqlStatement {
            sql: "INSERT INTO `accounts` (`id`, `name`) VALUES (?, ?)".to_string(),
            params: values(["1", "O'Reilly\\Books"]),
        })
        .expect("render statement");

        assert_eq!(
            rendered,
            "INSERT INTO `accounts` (`id`, `name`) VALUES ('1', 'O''Reilly\\\\Books')"
        );
    }

    #[test]
    fn renders_submitted_statement_bytes_as_binary_strings_without_utf8_loss() {
        let rendered = render_submitted_sql_statement(&SqlStatement {
            sql: "INSERT INTO `binary_values` (`payload`) VALUES (?)".to_string(),
            params: vec![Value::Bytes(vec![0x00, 0xff, b'\'', b'\\'])],
        })
        .expect("render submitted statement");

        assert_eq!(
            rendered,
            "INSERT INTO `binary_values` (`payload`) VALUES (_binary X'00ff275c')"
        );
    }

    #[test]
    fn rejects_non_finite_submitted_statement_numbers() {
        let error = render_submitted_sql_statement(&SqlStatement {
            sql: "INSERT INTO `measurements` (`reading`) VALUES (?)".to_string(),
            params: vec![Value::Double(f64::NAN)],
        })
        .expect_err("reject non-finite number");

        assert_eq!(
            error.to_string(),
            "submitted SQL cannot encode a non-finite DOUBLE"
        );
    }

    #[test]
    fn leaves_raw_sql_with_question_marks_unchanged_when_there_are_no_params() {
        let rendered = render_sql_statement(&SqlStatement {
            sql: "INSERT INTO `guests` (`reason`) VALUES (\"no guest cookies?\")".to_string(),
            params: Vec::new(),
        })
        .expect("render raw statement");

        assert_eq!(
            rendered,
            "INSERT INTO `guests` (`reason`) VALUES (\"no guest cookies?\")"
        );
    }

    #[test]
    fn rejects_sql_statement_placeholder_param_mismatch() {
        let error = render_sql_statement(&SqlStatement {
            sql: "SELECT ?, ?".to_string(),
            params: values(["one"]),
        })
        .expect_err("placeholder mismatch");

        assert_eq!(
            error.to_string(),
            "statement has 2 placeholders and 1 params"
        );
    }

    fn values<const N: usize>(items: [&str; N]) -> Vec<Value> {
        items
            .into_iter()
            .map(|item| Value::Bytes(item.as_bytes().to_vec()))
            .collect()
    }
}
