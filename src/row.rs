use crate::probe::BinlogCoordinate;
use crate::snapshot::SnapshotRow;
use crate::target::{PrimaryKey, TargetExecutor, TargetMySqlWriter, TargetWriteError};
use std::collections::BTreeMap;
use std::fmt;

pub type RowImage = BTreeMap<String, String>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowTableMap {
    pub table_id: u64,
    pub schema: String,
    pub table: String,
    pub columns: Vec<String>,
    pub primary_key: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableMapEvent {
    pub coordinate: BinlogCoordinate,
    pub table: RowTableMap,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRowsEvent {
    pub coordinate: BinlogCoordinate,
    pub table_id: u64,
    pub rows: Vec<RowImage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateRowsEvent {
    pub coordinate: BinlogCoordinate,
    pub table_id: u64,
    pub rows: Vec<RowUpdate>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteRowsEvent {
    pub coordinate: BinlogCoordinate,
    pub table_id: u64,
    pub rows: Vec<RowImage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

    fn table(&self, table_id: u64) -> Option<&RowTableMap> {
        self.tables.get(&table_id)
    }
}

pub struct RowApplier<E> {
    registry: TableMapRegistry,
    executor: E,
}

type RowResult<T> = Result<T, Box<RowApplyError>>;

impl<E> RowApplier<E>
where
    E: TargetExecutor,
{
    pub fn new(executor: E) -> Self {
        Self {
            registry: TableMapRegistry::default(),
            executor,
        }
    }

    pub fn apply_table_map(&mut self, event: TableMapEvent) {
        self.registry.apply_table_map(event);
    }

    pub fn apply_write_rows(&self, event: &WriteRowsEvent) -> RowResult<()> {
        let table = self.resolve_table(event.table_id, &event.coordinate)?;
        let rows = event
            .rows
            .iter()
            .map(|row| snapshot_row(table, row, &event.coordinate))
            .collect::<Result<Vec<_>, _>>()?;

        self.writer(table)
            .insert_rows(&rows)
            .map_err(|source| target_error(&event.coordinate, table, RowOperation::Insert, source))
    }

    pub fn apply_update_rows(&self, event: &UpdateRowsEvent) -> RowResult<()> {
        let table = self.resolve_table(event.table_id, &event.coordinate)?;
        let writer = self.writer(table);

        for update in &event.rows {
            let row = snapshot_row(table, &update.after, &event.coordinate)?;
            writer.update_row(&row).map_err(|source| {
                target_error(&event.coordinate, table, RowOperation::Update, source)
            })?;
        }

        Ok(())
    }

    pub fn apply_delete_rows(&self, event: &DeleteRowsEvent) -> RowResult<()> {
        let table = self.resolve_table(event.table_id, &event.coordinate)?;
        let writer = self.writer(table);

        for row in &event.rows {
            let primary_key = primary_key(table, row, &event.coordinate)?;
            writer.delete_row(&primary_key).map_err(|source| {
                target_error(&event.coordinate, table, RowOperation::Delete, source)
            })?;
        }

        Ok(())
    }

    pub fn executor(&self) -> &E {
        &self.executor
    }

    pub fn table_map(&self, table_id: u64) -> Option<&RowTableMap> {
        self.registry.table(table_id)
    }

    fn resolve_table(
        &self,
        table_id: u64,
        coordinate: &BinlogCoordinate,
    ) -> RowResult<&RowTableMap> {
        self.registry.table(table_id).ok_or_else(|| {
            row_error(RowApplyError::MissingTableMap {
                coordinate: coordinate.clone(),
                table_id,
            })
        })
    }

    fn writer<'a>(&'a self, table: &'a RowTableMap) -> TargetMySqlWriter<&'a E> {
        TargetMySqlWriter::new(
            table.table.clone(),
            table.primary_key.iter().map(String::as_str).collect(),
            table.columns.iter().map(String::as_str).collect(),
            &self.executor,
        )
    }
}

impl<E> TargetExecutor for &E
where
    E: TargetExecutor,
{
    fn execute(
        &self,
        statement: &crate::target::SqlStatement,
    ) -> Result<(), crate::target::TargetExecuteError> {
        (*self).execute(statement)
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
        source: Box<TargetWriteError>,
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
    source: &TargetWriteError,
) -> fmt::Result {
    write!(
        formatter,
        "failed to apply {operation} row event for {schema}.{table} at {}:{}: {source}",
        coordinate.file, coordinate.position
    )
}

fn snapshot_row(
    table: &RowTableMap,
    values: &RowImage,
    coordinate: &BinlogCoordinate,
) -> RowResult<SnapshotRow> {
    Ok(SnapshotRow {
        primary_key: primary_key_values(table, values, coordinate)?,
        values: values.clone(),
    })
}

fn primary_key(
    table: &RowTableMap,
    values: &RowImage,
    coordinate: &BinlogCoordinate,
) -> RowResult<PrimaryKey> {
    Ok(PrimaryKey::new(primary_key_values(
        table, values, coordinate,
    )?))
}

fn primary_key_values(
    table: &RowTableMap,
    values: &RowImage,
    coordinate: &BinlogCoordinate,
) -> RowResult<Vec<String>> {
    if table.primary_key.is_empty() {
        return Err(row_error(RowApplyError::MissingPrimaryKey {
            coordinate: coordinate.clone(),
            schema: table.schema.clone(),
            table: table.table.clone(),
        }));
    }

    table
        .primary_key
        .iter()
        .map(|column| primary_key_value(table, values, column, coordinate))
        .collect()
}

fn primary_key_value(
    table: &RowTableMap,
    values: &RowImage,
    column: &str,
    coordinate: &BinlogCoordinate,
) -> RowResult<String> {
    values.get(column).cloned().ok_or_else(|| {
        row_error(RowApplyError::MissingPrimaryKeyValue {
            coordinate: coordinate.clone(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            column: column.to_string(),
        })
    })
}

fn target_error(
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    operation: RowOperation,
    source: TargetWriteError,
) -> Box<RowApplyError> {
    row_error(RowApplyError::Target {
        coordinate: coordinate.clone(),
        schema: table.schema.clone(),
        table: table.table.clone(),
        operation,
        source: Box::new(source),
    })
}

fn row_error(error: RowApplyError) -> Box<RowApplyError> {
    Box::new(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::{SqlStatement, TargetExecuteError};
    use std::cell::RefCell;

    #[test]
    fn applies_write_rows_as_batched_insert() {
        let applier = applier_with_accounts_table();
        let event = WriteRowsEvent {
            coordinate: coordinate(120),
            table_id: 7,
            rows: vec![row("1", "alpha"), row("2", "beta")],
        };

        applier.apply_write_rows(&event).expect("apply write rows");

        let statements = applier.executor().statements.borrow();
        assert_eq!(statements.len(), 1);
        assert_eq!(
            statements[0].sql,
            "INSERT INTO `accounts` (`id`, `name`) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
        );
        assert_eq!(statements[0].params, vec!["1", "alpha", "2", "beta"]);
    }

    #[test]
    fn applies_update_rows_using_after_image_and_primary_key() {
        let applier = applier_with_accounts_table();
        let event = UpdateRowsEvent {
            coordinate: coordinate(140),
            table_id: 7,
            rows: vec![RowUpdate {
                before: row("1", "alpha"),
                after: row("1", "updated"),
            }],
        };

        applier
            .apply_update_rows(&event)
            .expect("apply update rows");

        let statements = applier.executor().statements.borrow();
        assert_eq!(statements.len(), 1);
        assert_eq!(
            statements[0].sql,
            "UPDATE `accounts` SET `name` = ? WHERE `id` = ?"
        );
        assert_eq!(statements[0].params, vec!["updated", "1"]);
    }

    #[test]
    fn applies_delete_rows_using_before_image_primary_key() {
        let applier = applier_with_accounts_table();
        let event = DeleteRowsEvent {
            coordinate: coordinate(160),
            table_id: 7,
            rows: vec![row("2", "beta")],
        };

        applier
            .apply_delete_rows(&event)
            .expect("apply delete rows");

        let statements = applier.executor().statements.borrow();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].sql, "DELETE FROM `accounts` WHERE `id` = ?");
        assert_eq!(statements[0].params, vec!["2"]);
    }

    #[test]
    fn rejects_row_event_without_table_map() {
        let applier = RowApplier::new(RecordingExecutor::default());
        let event = DeleteRowsEvent {
            coordinate: coordinate(160),
            table_id: 99,
            rows: vec![row("2", "beta")],
        };

        let error = applier
            .apply_delete_rows(&event)
            .expect_err("missing table map")
            .to_string();

        assert!(error.contains("missing table map"));
        assert!(error.contains("99"));
        assert!(error.contains("mysql-bin.000001:160"));
    }

    #[test]
    fn rejects_row_event_without_primary_key_value() {
        let applier = applier_with_accounts_table();
        let mut row = RowImage::new();
        row.insert("name".to_string(), "orphan".to_string());
        let event = WriteRowsEvent {
            coordinate: coordinate(180),
            table_id: 7,
            rows: vec![row],
        };

        let error = applier
            .apply_write_rows(&event)
            .expect_err("missing primary key")
            .to_string();

        assert!(error.contains("missing primary key column id"));
        assert!(error.contains("app.accounts"));
        assert!(error.contains("mysql-bin.000001:180"));
    }

    #[test]
    fn target_errors_include_operation_table_and_coordinate() {
        let executor = RecordingExecutor {
            error: Some(TargetExecuteError::new("deadlock")),
            ..RecordingExecutor::default()
        };
        let mut applier = RowApplier::new(executor);
        applier.apply_table_map(accounts_table_map());
        let event = DeleteRowsEvent {
            coordinate: coordinate(200),
            table_id: 7,
            rows: vec![row("2", "beta")],
        };

        let error = applier
            .apply_delete_rows(&event)
            .expect_err("target error")
            .to_string();

        assert!(error.contains("delete"));
        assert!(error.contains("app.accounts"));
        assert!(error.contains("mysql-bin.000001:200"));
        assert!(error.contains("deadlock"));
    }

    fn applier_with_accounts_table() -> RowApplier<RecordingExecutor> {
        let mut applier = RowApplier::new(RecordingExecutor::default());
        applier.apply_table_map(accounts_table_map());
        applier
    }

    fn accounts_table_map() -> TableMapEvent {
        TableMapEvent {
            coordinate: coordinate(100),
            table: RowTableMap {
                table_id: 7,
                schema: "app".to_string(),
                table: "accounts".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
                primary_key: vec!["id".to_string()],
            },
        }
    }

    fn row(id: &str, name: &str) -> RowImage {
        BTreeMap::from([
            ("id".to_string(), id.to_string()),
            ("name".to_string(), name.to_string()),
        ])
    }

    fn coordinate(position: u64) -> BinlogCoordinate {
        BinlogCoordinate {
            file: "mysql-bin.000001".to_string(),
            position,
        }
    }

    #[derive(Default)]
    struct RecordingExecutor {
        statements: RefCell<Vec<SqlStatement>>,
        error: Option<TargetExecuteError>,
    }

    impl TargetExecutor for RecordingExecutor {
        fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
            self.statements.borrow_mut().push(statement.clone());

            match &self.error {
                Some(error) => Err(error.clone()),
                None => Ok(()),
            }
        }
    }
}
