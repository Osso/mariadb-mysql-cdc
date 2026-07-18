use crate::checkpoint::Checkpoint;
use crate::snapshot::{SnapshotRow, SnapshotTarget};
use mysql::Value;
use std::fmt;

const MYSQL_MAX_PREPARED_STATEMENT_PLACEHOLDERS: usize = 65_535;

#[derive(Clone, Debug, PartialEq)]
pub struct SqlStatement {
    pub sql: String,
    pub params: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PrimaryKey {
    pub values: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotInsertMode {
    Upsert,
    IgnoreDuplicate,
}

impl PrimaryKey {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DuplicateConflict {
    pub error_code: u16,
    pub error_text: String,
    pub duplicate_index: Option<String>,
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
    pub table: String,
    pub primary_key_columns: Vec<String>,
    pub primary_key_values: Vec<Value>,
    pub writable_columns: Vec<String>,
    pub source_values: Vec<Value>,
    pub set_columns: Vec<Option<Vec<String>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetExecutionOutcome {
    Applied,
    DuplicateIgnored(DuplicateConflict),
    PrimaryKeyReplaced(DuplicateConflict),
    ConstraintConflict(DuplicateConflict),
}

pub trait TargetExecutor {
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError>;

    fn execute_row_change(
        &self,
        change: &TargetRowChange,
    ) -> Result<TargetExecutionOutcome, TargetExecuteError> {
        self.execute(&change.statement)?;
        Ok(TargetExecutionOutcome::Applied)
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

pub(crate) fn build_primary_key_replacement_statement(change: &TargetRowChange) -> SqlStatement {
    let assignments = change
        .writable_columns
        .iter()
        .map(|column| format!("{} = ?", crate::mysql_support::quote_ident(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let predicates = change
        .primary_key_columns
        .iter()
        .map(|column| format!("{} = ?", crate::mysql_support::quote_ident(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let mut params = change.source_values.clone();
    params.extend(change.primary_key_values.clone());
    SqlStatement {
        sql: format!(
            "UPDATE {} SET {assignments} WHERE {predicates}",
            crate::mysql_support::quote_ident(&change.table)
        ),
        params,
    }
}

pub(crate) fn primary_key_replacement_outcome(
    mut conflict: DuplicateConflict,
    existing_row_count: usize,
    affected_row_count: u64,
) -> TargetExecutionOutcome {
    if existing_row_count == 1 && affected_row_count == 1 {
        return TargetExecutionOutcome::PrimaryKeyReplaced(conflict);
    }

    conflict.error_text = format!(
        "replace-divergent-pk: expected exactly one existing PK row and one affected target row, got existing_rows={existing_row_count} affected_rows={affected_row_count}; {}",
        conflict.error_text
    );
    TargetExecutionOutcome::ConstraintConflict(conflict)
}

pub(crate) fn duplicate_insert_outcome(
    conflict: DuplicateConflict,
    existing_values: Option<&[Value]>,
    source_values: &[Value],
    set_columns: &[Option<Vec<String>>],
) -> TargetExecutionOutcome {
    if existing_values.is_some_and(|values| values_equal(values, source_values, set_columns)) {
        TargetExecutionOutcome::DuplicateIgnored(conflict)
    } else {
        TargetExecutionOutcome::ConstraintConflict(conflict)
    }
}

fn values_equal(left: &[Value], right: &[Value], set_columns: &[Option<Vec<String>>]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .enumerate()
            .all(|(index, (left, right))| {
                value_equal(
                    left,
                    right,
                    set_columns.get(index).and_then(Option::as_deref),
                )
            })
}

fn value_equal(left: &Value, right: &Value, set_values: Option<&[String]>) -> bool {
    if let Some(set_values) = set_values {
        return set_values_equal(left, right, set_values);
    }
    value_equal_without_set(left, right)
}

fn set_values_equal(left: &Value, right: &Value, set_values: &[String]) -> bool {
    match (left, right) {
        (Value::UInt(left), Value::Bytes(right)) => set_mask_matches_text(*left, right, set_values),
        (Value::Bytes(left), Value::UInt(right)) => set_mask_matches_text(*right, left, set_values),
        _ => value_equal_without_set(left, right),
    }
}

fn set_mask_matches_text(mask: u64, text: &[u8], set_values: &[String]) -> bool {
    let Ok(text) = std::str::from_utf8(text) else {
        return false;
    };
    if set_values.len() > u64::BITS as usize {
        return false;
    }
    let allowed_mask = if set_values.len() == u64::BITS as usize {
        u64::MAX
    } else {
        (1_u64 << set_values.len()) - 1
    };
    if mask & !allowed_mask != 0 {
        return false;
    }
    if text.is_empty() {
        return mask == 0;
    }

    let mut text_mask = 0_u64;
    for value in text.split(',') {
        let Some(index) = set_values.iter().position(|candidate| candidate == value) else {
            return false;
        };
        let bit = 1_u64 << index;
        if text_mask & bit != 0 {
            return false;
        }
        text_mask |= bit;
    }
    text_mask == mask
}

fn value_equal_without_set(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::NULL, Value::NULL) => true,
        (Value::Bytes(left), Value::Bytes(right)) => left == right,
        (Value::Date(..), Value::Date(..))
        | (Value::Time(..), Value::Time(..))
        | (Value::Date(..), Value::Bytes(_))
        | (Value::Bytes(_), Value::Date(..))
        | (Value::Time(..), Value::Bytes(_))
        | (Value::Bytes(_), Value::Time(..)) => temporal_values_equal(left, right),
        (Value::Float(left), Value::Float(right)) => left == right,
        (Value::Double(left), Value::Double(right)) => left == right,
        (left, right) if is_numeric_value(left) || is_numeric_value(right) => numeric_value(left)
            .zip(numeric_value(right))
            .is_some_and(|(left, right)| left == right),
        _ => left == right,
    }
}

fn is_numeric_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Int(_) | Value::UInt(_) | Value::Float(_) | Value::Double(_)
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalNumber {
    negative: bool,
    digits: String,
    scale: i64,
}

fn numeric_value(value: &Value) -> Option<CanonicalNumber> {
    let text = match value {
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::Bytes(value) => std::str::from_utf8(value).ok()?.to_string(),
        _ => return None,
    };
    parse_numeric_text(&text)
}

fn parse_numeric_text(text: &str) -> Option<CanonicalNumber> {
    let (negative, text) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let (mantissa, exponent) = text
        .split_once(['e', 'E'])
        .map_or((text, 0), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i64>().ok().unwrap_or(i64::MAX))
        });
    if mantissa.is_empty() || exponent == i64::MAX {
        return None;
    }

    let mut digits = String::new();
    let mut fractional_digits = 0_i64;
    let mut seen_decimal = false;
    for character in mantissa.chars() {
        match character {
            '0'..='9' => {
                digits.push(character);
                fractional_digits += i64::from(seen_decimal);
            }
            '.' if !seen_decimal => seen_decimal = true,
            _ => return None,
        }
    }
    if digits.is_empty() {
        return None;
    }

    let first_nonzero = digits.find(|character| character != '0');
    let Some(first_nonzero) = first_nonzero else {
        return Some(CanonicalNumber {
            negative: false,
            digits: "0".to_string(),
            scale: 0,
        });
    };
    digits.drain(..first_nonzero);
    while digits.ends_with('0') {
        digits.pop();
        fractional_digits -= 1;
    }

    Some(CanonicalNumber {
        negative,
        digits,
        scale: fractional_digits - exponent,
    })
}

fn temporal_values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (
            Value::Date(
                left_year,
                left_month,
                left_day,
                left_hour,
                left_minute,
                left_second,
                left_micros,
            ),
            Value::Date(
                right_year,
                right_month,
                right_day,
                right_hour,
                right_minute,
                right_second,
                right_micros,
            ),
        ) => {
            (
                left_year,
                left_month,
                left_day,
                left_hour,
                left_minute,
                left_second,
                left_micros,
            ) == (
                right_year,
                right_month,
                right_day,
                right_hour,
                right_minute,
                right_second,
                right_micros,
            )
        }
        (
            Value::Time(
                left_negative,
                left_days,
                left_hours,
                left_minutes,
                left_seconds,
                left_micros,
            ),
            Value::Time(
                right_negative,
                right_days,
                right_hours,
                right_minutes,
                right_seconds,
                right_micros,
            ),
        ) => {
            (
                left_negative,
                left_days,
                left_hours,
                left_minutes,
                left_seconds,
                left_micros,
            ) == (
                right_negative,
                right_days,
                right_hours,
                right_minutes,
                right_seconds,
                right_micros,
            )
        }
        (Value::Date(..), Value::Bytes(bytes)) => {
            parse_date_text(bytes).is_some_and(|value| temporal_date(left) == value)
        }
        (Value::Bytes(bytes), Value::Date(..)) => {
            parse_date_text(bytes).is_some_and(|value| value == temporal_date(right))
        }
        (Value::Time(..), Value::Bytes(bytes)) => {
            parse_time_text(bytes).is_some_and(|value| temporal_time(left) == value)
        }
        (Value::Bytes(bytes), Value::Time(..)) => {
            parse_time_text(bytes).is_some_and(|value| value == temporal_time(right))
        }
        _ => false,
    }
}

type DateParts = (u16, u8, u8, u8, u8, u8, u32);
type TimeParts = (bool, u32, u8, u8, u8, u32);

fn temporal_date(value: &Value) -> DateParts {
    match value {
        Value::Date(year, month, day, hour, minute, second, micros) => {
            (*year, *month, *day, *hour, *minute, *second, *micros)
        }
        _ => unreachable!("expected date value"),
    }
}

fn temporal_time(value: &Value) -> TimeParts {
    match value {
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            (*negative, *days, *hours, *minutes, *seconds, *micros)
        }
        _ => unreachable!("expected time value"),
    }
}

fn parse_date_text(bytes: &[u8]) -> Option<DateParts> {
    let text = std::str::from_utf8(bytes).ok()?;
    let (date, time) = text
        .split_once(' ')
        .map_or((text, None), |(date, time)| (date, Some(time)));
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse().ok()?;
    let month = date_parts.next()?.parse().ok()?;
    let day = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let (hour, minute, second, micros) = match time {
        Some(time) => parse_clock_text(time)?,
        None => (0, 0, 0, 0),
    };
    Some((
        year,
        month,
        day,
        hour.try_into().ok()?,
        minute,
        second,
        micros,
    ))
}

fn parse_time_text(bytes: &[u8]) -> Option<TimeParts> {
    let text = std::str::from_utf8(bytes).ok()?;
    let (negative, text) = text
        .strip_prefix('-')
        .map_or((false, text), |text| (true, text));
    let (total_hours, minutes, seconds, micros) = parse_clock_text(text)?;
    Some((
        negative,
        total_hours / 24,
        (total_hours % 24).try_into().ok()?,
        minutes,
        seconds,
        micros,
    ))
}

fn parse_clock_text(text: &str) -> Option<(u32, u8, u8, u32)> {
    let (clock, fraction) = text
        .split_once('.')
        .map_or((text, None), |(clock, fraction)| (clock, Some(fraction)));
    let mut parts = clock.split(':');
    let hours = parts.next()?.parse().ok()?;
    let minutes = parts.next()?.parse().ok()?;
    let seconds = parts.next()?.parse().ok()?;
    if parts.next().is_some() || minutes >= 60 || seconds >= 60 {
        return None;
    }
    let micros = match fraction {
        None => 0,
        Some(fraction)
            if !fraction.is_empty()
                && fraction.len() <= 6
                && fraction.chars().all(|c| c.is_ascii_digit()) =>
        {
            format!("{fraction:0<6}").parse().ok()?
        }
        Some(_) => return None,
    };
    Some((hours, minutes, seconds, micros))
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

#[derive(Clone, Debug)]
pub struct TargetMySqlWriter<E> {
    table: String,
    primary_key: Vec<String>,
    columns: Vec<String>,
    insert_mode: SnapshotInsertMode,
    pub executor: E,
}

impl<E> TargetMySqlWriter<E>
where
    E: TargetExecutor,
{
    pub fn new(
        table: impl Into<String>,
        primary_key: Vec<&str>,
        columns: Vec<&str>,
        executor: E,
    ) -> Self {
        Self {
            table: table.into(),
            primary_key: primary_key.into_iter().map(str::to_string).collect(),
            columns: columns.into_iter().map(str::to_string).collect(),
            insert_mode: SnapshotInsertMode::Upsert,
            executor,
        }
    }

    pub fn from_snapshot_table(
        table: &crate::snapshot::SnapshotTable,
        executor: E,
        insert_mode: SnapshotInsertMode,
    ) -> Self {
        Self {
            table: table.name.clone(),
            primary_key: table.primary_key.clone(),
            columns: table.columns.clone(),
            insert_mode,
            executor,
        }
    }

    pub fn insert_rows(&self, rows: &[SnapshotRow]) -> Result<(), TargetWriteError> {
        if rows.is_empty() {
            return Ok(());
        }

        for batch in rows.chunks(max_insert_rows_per_statement(self.columns.len())) {
            self.insert_row_batch(batch)?;
        }
        Ok(())
    }

    fn insert_row_batch(&self, rows: &[SnapshotRow]) -> Result<(), TargetWriteError> {
        let statement = build_insert_statement(
            &self.table,
            &self.primary_key,
            &self.columns,
            rows,
            self.insert_mode,
        );
        self.execute_with_context("insert", rows.len(), statement)
    }

    pub fn update_row(&self, row: &SnapshotRow) -> Result<(), TargetWriteError> {
        let statement = build_update_statement(&self.table, &self.primary_key, &self.columns, row);
        self.execute_with_context("update", 1, statement)
    }

    pub fn delete_row(&self, primary_key: &PrimaryKey) -> Result<(), TargetWriteError> {
        let statement = build_delete_statement(&self.table, &self.primary_key, primary_key);
        self.execute_with_context("delete", 1, statement)
    }

    fn execute_with_context(
        &self,
        operation: &'static str,
        row_count: usize,
        statement: SqlStatement,
    ) -> Result<(), TargetWriteError> {
        self.executor
            .execute(&statement)
            .map_err(|source| TargetWriteError {
                operation,
                table: self.table.clone(),
                row_count,
                sql: statement.sql,
                source,
            })
    }
}

impl<E> SnapshotTarget for TargetMySqlWriter<E>
where
    E: TargetExecutor,
{
    fn write_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), crate::snapshot::SnapshotError> {
        self.insert_rows(rows)
            .map_err(|error| crate::snapshot::SnapshotError::InvalidTable(error.to_string()))
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

#[derive(Debug)]
pub struct TargetWriteError {
    operation: &'static str,
    table: String,
    row_count: usize,
    sql: String,
    source: TargetExecuteError,
}

impl fmt::Display for TargetWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rows = match self.row_count {
            1 => "1 row".to_string(),
            count => format!("{count} rows"),
        };

        write!(
            formatter,
            "target {} failed for {} on {}: {}; sql: {}",
            self.operation, rows, self.table, self.source, self.sql
        )
    }
}

impl std::error::Error for TargetWriteError {}

pub fn duplicate_index_from_error(error_text: &str) -> Option<String> {
    let marker = " for key '";
    let start = error_text.find(marker)? + marker.len();
    let remainder = &error_text[start..];
    let end = remainder.find('\'')?;
    let key = &remainder[..end];
    (!key.is_empty()).then(|| key.to_string())
}

pub fn render_sql_statement(statement: &SqlStatement) -> Result<String, TargetExecuteError> {
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
            rendered.push_str(&quote_sql_value(param));
        }
    }

    Ok(rendered)
}

fn build_insert_statement(
    table: &str,
    primary_key: &[String],
    columns: &[String],
    rows: &[SnapshotRow],
    insert_mode: SnapshotInsertMode,
) -> SqlStatement {
    let column_list = columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>();
    let placeholders = row_placeholders(columns.len(), rows.len());
    let update_list = update_assignments(columns, primary_key);
    let params = rows
        .iter()
        .flat_map(|row| ordered_values(row, columns))
        .collect();

    let sql = match insert_mode {
        SnapshotInsertMode::Upsert => format!(
            "INSERT INTO {} ({}) VALUES {} ON DUPLICATE KEY UPDATE {}",
            quote_ident(table),
            column_list.join(", "),
            placeholders,
            update_list.join(", ")
        ),
        SnapshotInsertMode::IgnoreDuplicate => format!(
            "INSERT IGNORE INTO {} ({}) VALUES {}",
            quote_ident(table),
            column_list.join(", "),
            placeholders
        ),
    };

    SqlStatement { sql, params }
}

fn max_insert_rows_per_statement(column_count: usize) -> usize {
    let divisor = column_count.max(1);
    (MYSQL_MAX_PREPARED_STATEMENT_PLACEHOLDERS / divisor).max(1)
}

fn build_update_statement(
    table: &str,
    primary_key: &[String],
    columns: &[String],
    row: &SnapshotRow,
) -> SqlStatement {
    let changed_columns = non_primary_columns(columns, primary_key);
    let assignments = changed_columns
        .iter()
        .map(|column| format!("{} = ?", quote_ident(column)))
        .collect::<Vec<_>>();
    let predicates = primary_key_predicates(primary_key);
    let mut params = ordered_values(row, &changed_columns);
    params.extend(row.primary_key.iter().cloned().map(string_param));

    SqlStatement {
        sql: format!(
            "UPDATE {} SET {} WHERE {}",
            quote_ident(table),
            assignments.join(", "),
            predicates.join(" AND ")
        ),
        params,
    }
}

fn build_delete_statement(
    table: &str,
    primary_key: &[String],
    row_primary_key: &PrimaryKey,
) -> SqlStatement {
    SqlStatement {
        sql: format!(
            "DELETE FROM {} WHERE {}",
            quote_ident(table),
            primary_key_predicates(primary_key).join(" AND ")
        ),
        params: row_primary_key.values.clone(),
    }
}

fn ordered_values(row: &SnapshotRow, columns: &[String]) -> Vec<Value> {
    columns
        .iter()
        .map(|column| match row.values.get(column).cloned().flatten() {
            Some(value) => string_param(value),
            None => Value::NULL,
        })
        .collect()
}

fn row_placeholders(column_count: usize, row_count: usize) -> String {
    let row_placeholder = format!("({})", vec!["?"; column_count].join(", "));
    vec![row_placeholder; row_count].join(", ")
}

fn update_assignments(columns: &[String], primary_key: &[String]) -> Vec<String> {
    non_primary_columns(columns, primary_key)
        .iter()
        .map(|column| format!("{} = VALUES({})", quote_ident(column), quote_ident(column)))
        .collect()
}

fn non_primary_columns(columns: &[String], primary_key: &[String]) -> Vec<String> {
    columns
        .iter()
        .filter(|column| !primary_key.contains(column))
        .cloned()
        .collect()
}

fn primary_key_predicates(primary_key: &[String]) -> Vec<String> {
    primary_key
        .iter()
        .map(|column| format!("{} = ?", quote_ident(column)))
        .collect()
}

fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn string_param(value: String) -> Value {
    Value::Bytes(value.into_bytes())
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

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{SnapshotRow, SnapshotTarget};
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[test]
    fn writes_snapshot_rows_as_batched_upsert() {
        let executor = RecordingExecutor::default();
        let mut writer =
            TargetMySqlWriter::new("accounts", vec!["id"], vec!["id", "name"], executor);

        writer
            .write_rows(&[row("1", "alpha"), row("2", "beta")])
            .expect("write rows");

        let executed = writer.executor.statements.borrow();
        assert_eq!(executed.len(), 1);
        assert_eq!(
            executed[0].sql,
            "INSERT INTO `accounts` (`id`, `name`) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
        );
        assert_eq!(executed[0].params, values(["1", "alpha", "2", "beta"]));
    }

    #[test]
    fn writes_null_snapshot_values_as_sql_null() {
        let executor = RecordingExecutor::default();
        let writer = TargetMySqlWriter::new(
            "access_tokens",
            vec!["id"],
            vec!["id", "artist_id", "name"],
            executor,
        );
        let row = SnapshotRow {
            primary_key: vec!["7".to_string()],
            values: BTreeMap::from([
                ("id".to_string(), Some("7".to_string())),
                ("artist_id".to_string(), None),
                ("name".to_string(), Some("NULL".to_string())),
            ]),
        };

        writer.update_row(&row).expect("update row");

        assert_eq!(
            writer.executor.statements.borrow()[0].params,
            vec![
                Value::NULL,
                Value::Bytes(b"NULL".to_vec()),
                Value::Bytes(b"7".to_vec())
            ]
        );
    }

    #[test]
    fn writes_updates_and_deletes_with_primary_key_predicates() {
        let executor = RecordingExecutor::default();
        let writer = TargetMySqlWriter::new("accounts", vec!["id"], vec!["id", "name"], executor);

        writer.update_row(&row("7", "updated")).expect("update row");
        writer
            .delete_row(&PrimaryKey::new(values(["7"])))
            .expect("delete row");

        let executed = writer.executor.statements.borrow();
        assert_eq!(
            executed[0].sql,
            "UPDATE `accounts` SET `name` = ? WHERE `id` = ?"
        );
        assert_eq!(executed[0].params, values(["updated", "7"]));
        assert_eq!(executed[1].sql, "DELETE FROM `accounts` WHERE `id` = ?");
        assert_eq!(executed[1].params, values(["7"]));
    }

    #[test]
    fn writes_snapshot_rows_as_insert_ignore_when_requested() {
        let executor = RecordingExecutor::default();
        let table = crate::snapshot::SnapshotTable {
            name: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
        };
        let mut writer = TargetMySqlWriter::from_snapshot_table(
            &table,
            executor,
            SnapshotInsertMode::IgnoreDuplicate,
        );

        writer.write_rows(&[row("1", "alpha")]).expect("write rows");

        let executed = writer.executor.statements.borrow();
        assert_eq!(
            executed[0].sql,
            "INSERT IGNORE INTO `accounts` (`id`, `name`) VALUES (?, ?)"
        );
        assert_eq!(executed[0].params, values(["1", "alpha"]));
    }

    #[test]
    fn splits_insert_batches_under_mysql_placeholder_limit() {
        let executor = RecordingExecutor::default();
        let columns = numbered_columns(21);
        let rows = (0..3121)
            .map(|row_number| wide_row(row_number, &columns))
            .collect::<Vec<_>>();
        let writer = TargetMySqlWriter::new(
            "wide_accounts",
            vec!["c0"],
            columns.iter().map(String::as_str).collect(),
            executor,
        );

        writer.insert_rows(&rows).expect("insert rows");

        let executed = writer.executor.statements.borrow();
        assert_eq!(executed.len(), 2);
        assert_eq!(executed[0].params.len(), 65_520);
        assert_eq!(executed[1].params.len(), 21);
    }

    #[test]
    fn builds_primary_key_replacement_update_from_source_image() {
        let change = TargetRowChange {
            statement: SqlStatement {
                sql: "INSERT INTO `accounts` (`id`, `name`) VALUES (?, ?)".to_string(),
                params: values(["7", "source"]),
            },
            kind: TargetRowChangeKind::Insert,
            table: "accounts".to_string(),
            primary_key_columns: vec!["id".to_string()],
            primary_key_values: values(["7"]),
            writable_columns: vec!["id".to_string(), "name".to_string()],
            source_values: values(["7", "source"]),
            set_columns: vec![None, None],
        };

        let replacement = build_primary_key_replacement_statement(&change);

        assert_eq!(
            replacement.sql,
            "UPDATE `accounts` SET `id` = ?, `name` = ? WHERE `id` = ?"
        );
        assert_eq!(replacement.params, values(["7", "source", "7"]));
    }

    #[test]
    fn replacement_requires_exactly_one_existing_pk_row_and_one_affected_target_row() {
        let conflict = DuplicateConflict {
            error_code: 1062,
            error_text: "Duplicate entry '7' for key 'PRIMARY'".to_string(),
            duplicate_index: Some("PRIMARY".to_string()),
        };

        for (existing_rows, affected_rows) in [(0, 0), (2, 1), (1, 0), (1, 2)] {
            let outcome =
                primary_key_replacement_outcome(conflict.clone(), existing_rows, affected_rows);
            let TargetExecutionOutcome::ConstraintConflict(conflict) = outcome else {
                panic!("replacement precondition unexpectedly succeeded");
            };
            assert!(conflict.error_text.starts_with("replace-divergent-pk:"));
        }
        assert_eq!(
            primary_key_replacement_outcome(conflict.clone(), 1, 1),
            TargetExecutionOutcome::PrimaryKeyReplaced(conflict)
        );
    }

    #[test]
    fn duplicate_insert_continues_only_for_type_aware_equal_values() {
        let conflict = DuplicateConflict {
            error_code: 1062,
            error_text: "duplicate".to_string(),
            duplicate_index: Some("PRIMARY".to_string()),
        };
        let source_values = vec![Value::UInt(7), Value::Bytes(b"same".to_vec())];

        assert_eq!(
            duplicate_insert_outcome(conflict.clone(), Some(&source_values), &source_values, &[],),
            TargetExecutionOutcome::DuplicateIgnored(conflict.clone())
        );
        assert_eq!(
            duplicate_insert_outcome(
                conflict.clone(),
                Some(&[Value::Int(7), Value::Bytes(b"same".to_vec())]),
                &source_values,
                &[],
            ),
            TargetExecutionOutcome::DuplicateIgnored(conflict.clone())
        );
        assert_eq!(
            duplicate_insert_outcome(
                conflict.clone(),
                Some(&[
                    Value::Double(12.34),
                    Value::Date(2026, 7, 16, 3, 4, 5, 600_000),
                    Value::Time(false, 1, 2, 3, 4, 600_000),
                ]),
                &[
                    Value::Bytes(b"12.3400".to_vec()),
                    Value::Bytes(b"2026-07-16 03:04:05.600".to_vec()),
                    Value::Bytes(b"26:03:04.600".to_vec()),
                ],
                &[],
            ),
            TargetExecutionOutcome::DuplicateIgnored(conflict.clone())
        );
        assert_eq!(
            duplicate_insert_outcome(
                conflict.clone(),
                Some(&[Value::Bytes(b"7".to_vec()), Value::Bytes(b"same".to_vec())]),
                &[
                    Value::Bytes(b"007".to_vec()),
                    Value::Bytes(b"same".to_vec())
                ],
                &[],
            ),
            TargetExecutionOutcome::ConstraintConflict(conflict.clone())
        );
        assert_eq!(
            duplicate_insert_outcome(conflict.clone(), None, &source_values, &[]),
            TargetExecutionOutcome::ConstraintConflict(conflict)
        );
    }

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

    #[test]
    fn error_contains_operation_table_row_count_and_sql() {
        let executor = RecordingExecutor::failing("duplicate key");
        let mut writer =
            TargetMySqlWriter::new("accounts", vec!["id"], vec!["id", "name"], executor);

        let error = writer
            .write_rows(&[row("1", "alpha")])
            .expect_err("write failure");
        let message = error.to_string();

        assert!(message.contains("insert"));
        assert!(message.contains("accounts"));
        assert!(message.contains("1 row"));
        assert!(message.contains("duplicate key"));
        assert!(message.contains("INSERT INTO `accounts`"));
    }

    fn row(id: &str, name: &str) -> SnapshotRow {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), Some(id.to_string()));
        values.insert("name".to_string(), Some(name.to_string()));

        SnapshotRow {
            primary_key: vec![id.to_string()],
            values,
        }
    }

    fn values<const N: usize>(items: [&str; N]) -> Vec<Value> {
        items
            .into_iter()
            .map(|item| Value::Bytes(item.as_bytes().to_vec()))
            .collect()
    }

    fn numbered_columns(count: usize) -> Vec<String> {
        (0..count).map(|index| format!("c{index}")).collect()
    }

    fn wide_row(row_number: usize, columns: &[String]) -> SnapshotRow {
        let values = columns
            .iter()
            .map(|column| (column.clone(), Some(format!("{column}-{row_number}"))))
            .collect();

        SnapshotRow {
            primary_key: vec![row_number.to_string()],
            values,
        }
    }

    #[derive(Default)]
    struct RecordingExecutor {
        statements: RefCell<Vec<SqlStatement>>,
        failure: Option<String>,
    }

    impl RecordingExecutor {
        fn failing(message: &str) -> Self {
            Self {
                statements: RefCell::new(Vec::new()),
                failure: Some(message.to_string()),
            }
        }
    }

    impl TargetExecutor for RecordingExecutor {
        fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
            self.statements.borrow_mut().push(statement.clone());

            match &self.failure {
                Some(message) => Err(TargetExecuteError::new(message)),
                None => Ok(()),
            }
        }
    }
}
