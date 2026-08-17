use crate::probe::BinlogCoordinate;
use crate::target::TargetExecuteError;
use mysql::Value;
use std::collections::BTreeMap;
use std::fmt;

pub type RowImage = BTreeMap<String, Value>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowTableMap {
    pub table_id: u64,
    pub schema: String,
    pub table: String,
    pub columns: Vec<String>,
    pub primary_key: Vec<String>,
    pub generated_columns: Vec<String>,
    pub signed_columns: Vec<String>,
    pub enum_columns: BTreeMap<String, Vec<String>>,
    pub set_columns: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableMapEvent {
    pub coordinate: BinlogCoordinate,
    pub table: RowTableMap,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteRowsEvent {
    pub coordinate: BinlogCoordinate,
    pub table_id: u64,
    pub rows: Vec<RowImage>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateRowsEvent {
    pub coordinate: BinlogCoordinate,
    pub table_id: u64,
    pub rows: Vec<RowUpdate>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeleteRowsEvent {
    pub coordinate: BinlogCoordinate,
    pub table_id: u64,
    pub rows: Vec<RowImage>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowUpdate {
    pub before: RowImage,
    pub after: RowImage,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableMapRegistry {
    tables: BTreeMap<u64, RowTableMap>,
}

impl TableMapRegistry {
    pub fn apply_table_map(&mut self, event: TableMapEvent) {
        self.tables.insert(event.table.table_id, event.table);
    }

    pub(crate) fn table(&self, table_id: u64) -> Option<&RowTableMap> {
        self.tables.get(&table_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowOperation {
    Insert,
    Update,
    Delete,
}

impl fmt::Display for RowOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert => formatter.write_str("insert"),
            Self::Update => formatter.write_str("update"),
            Self::Delete => formatter.write_str("delete"),
        }
    }
}

#[derive(Debug)]
pub enum RowApplyError {
    MissingTableMap {
        coordinate: BinlogCoordinate,
        table_id: u64,
    },
    MissingPrimaryKey {
        coordinate: BinlogCoordinate,
        schema: String,
        table: String,
    },
    MissingPrimaryKeyValue {
        coordinate: BinlogCoordinate,
        schema: String,
        table: String,
        column: String,
    },
    Target {
        coordinate: BinlogCoordinate,
        schema: String,
        table: String,
        operation: RowOperation,
        source: TargetExecuteError,
    },
}

impl fmt::Display for RowApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingTableMap {
                coordinate,
                table_id,
            } => write_missing_table_map(formatter, coordinate, *table_id),
            Self::MissingPrimaryKey {
                coordinate,
                schema,
                table,
            } => write_missing_primary_key(formatter, coordinate, schema, table),
            Self::MissingPrimaryKeyValue {
                coordinate,
                schema,
                table,
                column,
            } => write_missing_primary_key_value(formatter, coordinate, schema, table, column),
            Self::Target {
                coordinate,
                schema,
                table,
                operation,
                source,
            } => write_target_error(formatter, coordinate, schema, table, *operation, source),
        }
    }
}

impl std::error::Error for RowApplyError {}

pub(crate) type RowResult<T> = Result<T, Box<RowApplyError>>;

pub(crate) fn row_error(error: RowApplyError) -> Box<RowApplyError> {
    Box::new(error)
}

fn write_missing_table_map(
    formatter: &mut fmt::Formatter<'_>,
    coordinate: &BinlogCoordinate,
    table_id: u64,
) -> fmt::Result {
    write!(
        formatter,
        "missing table map for table id {table_id} at {}:{}",
        coordinate.file, coordinate.position
    )
}

fn write_missing_primary_key(
    formatter: &mut fmt::Formatter<'_>,
    coordinate: &BinlogCoordinate,
    schema: &str,
    table: &str,
) -> fmt::Result {
    write!(
        formatter,
        "row event for {schema}.{table} has no primary key at {}:{}",
        coordinate.file, coordinate.position
    )
}

fn write_missing_primary_key_value(
    formatter: &mut fmt::Formatter<'_>,
    coordinate: &BinlogCoordinate,
    schema: &str,
    table: &str,
    column: &str,
) -> fmt::Result {
    write!(
        formatter,
        "row event for {schema}.{table} missing primary key column {column} at {}:{}",
        coordinate.file, coordinate.position
    )
}

fn write_target_error(
    formatter: &mut fmt::Formatter<'_>,
    coordinate: &BinlogCoordinate,
    schema: &str,
    table: &str,
    operation: RowOperation,
    source: &TargetExecuteError,
) -> fmt::Result {
    write!(
        formatter,
        "failed to apply {operation} row event for {schema}.{table} at {}:{}: {source}",
        coordinate.file, coordinate.position
    )
}
