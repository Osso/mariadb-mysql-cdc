use super::model::{
    DeleteRowsEvent, RowApplyError, RowImage, RowOperation, RowResult, RowTableMap, RowUpdate,
    TableMapEvent, TableMapRegistry, UpdateRowsEvent, WriteRowsEvent, row_error,
};
use super::sql::{
    build_delete_statement, build_insert_statement, build_update_statement, primary_key_values,
    validate_row_has_primary_key, validate_rows_have_primary_keys,
};
use crate::probe::BinlogCoordinate;
use crate::target::{
    SqlStatement, TargetExecuteError, TargetExecutor, TargetRowChange, TargetRowChangeKind,
};

type RowPreflight<R> = fn(&RowTableMap, &[R], &BinlogCoordinate) -> RowResult<()>;
type RowBuilder<R> = fn(&RowTableMap, &R, &BinlogCoordinate) -> RowResult<Option<TargetRowChange>>;

pub struct RowApplier<E> {
    registry: TableMapRegistry,
    executor: E,
}

struct RowEventInput<'a, R> {
    table_id: u64,
    coordinate: &'a BinlogCoordinate,
    rows: &'a [R],
    operation: RowOperation,
    preflight: RowPreflight<R>,
    build_change: RowBuilder<R>,
}

impl<'a, R> RowEventInput<'a, R> {
    fn new(
        table_id: u64,
        coordinate: &'a BinlogCoordinate,
        rows: &'a [R],
        operation: RowOperation,
        preflight: RowPreflight<R>,
        build_change: RowBuilder<R>,
    ) -> Self {
        Self {
            table_id,
            coordinate,
            rows,
            operation,
            preflight,
            build_change,
        }
    }
}

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
        self.apply_row_event(write_rows_input(event))
    }

    pub fn apply_update_rows(&self, event: &UpdateRowsEvent) -> RowResult<()> {
        self.apply_row_event(update_rows_input(event))
    }

    pub fn apply_delete_rows(&self, event: &DeleteRowsEvent) -> RowResult<()> {
        self.apply_row_event(delete_rows_input(event))
    }

    pub fn executor(&self) -> &E {
        &self.executor
    }

    pub fn table_map(&self, table_id: u64) -> Option<&RowTableMap> {
        self.registry.table(table_id)
    }

    fn apply_row_event<R>(&self, input: RowEventInput<'_, R>) -> RowResult<()> {
        let table = self.resolve_table(input.table_id, input.coordinate)?;
        (input.preflight)(table, input.rows, input.coordinate)?;

        for row in input.rows {
            let Some(change) = (input.build_change)(table, row, input.coordinate)? else {
                continue;
            };
            self.executor
                .execute_row_change(&change)
                .map_err(|source| {
                    row_error(RowApplyError::Target {
                        coordinate: input.coordinate.clone(),
                        schema: table.schema.clone(),
                        table: table.table.clone(),
                        operation: input.operation,
                        source,
                    })
                })?;
        }

        Ok(())
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
}

fn write_rows_input(event: &WriteRowsEvent) -> RowEventInput<'_, RowImage> {
    RowEventInput::new(
        event.table_id,
        &event.coordinate,
        &event.rows,
        RowOperation::Insert,
        validate_rows_have_primary_keys,
        insert_change,
    )
}

fn update_rows_input(event: &UpdateRowsEvent) -> RowEventInput<'_, RowUpdate> {
    RowEventInput::new(
        event.table_id,
        &event.coordinate,
        &event.rows,
        RowOperation::Update,
        no_preflight,
        update_change,
    )
}

fn delete_rows_input(event: &DeleteRowsEvent) -> RowEventInput<'_, RowImage> {
    RowEventInput::new(
        event.table_id,
        &event.coordinate,
        &event.rows,
        RowOperation::Delete,
        no_preflight,
        delete_change,
    )
}

fn no_preflight<R>(
    _table: &RowTableMap,
    _rows: &[R],
    _coordinate: &BinlogCoordinate,
) -> RowResult<()> {
    Ok(())
}

fn insert_change(
    table: &RowTableMap,
    row: &RowImage,
    coordinate: &BinlogCoordinate,
) -> RowResult<Option<TargetRowChange>> {
    primary_key_values(table, row, coordinate)?;
    Ok(Some(TargetRowChange {
        statement: build_insert_statement(table, row),
        kind: TargetRowChangeKind::Insert,
        schema: table.schema.clone(),
        table: table.table.clone(),
        values: row.clone(),
    }))
}

fn update_change(
    table: &RowTableMap,
    update: &RowUpdate,
    coordinate: &BinlogCoordinate,
) -> RowResult<Option<TargetRowChange>> {
    validate_row_has_primary_key(table, &update.after, coordinate)?;
    let Some(statement) = build_update_statement(table, update, coordinate)? else {
        return Ok(None);
    };
    Ok(Some(TargetRowChange {
        statement,
        kind: TargetRowChangeKind::Update,
        schema: table.schema.clone(),
        table: table.table.clone(),
        values: update.after.clone(),
    }))
}

fn delete_change(
    table: &RowTableMap,
    row: &RowImage,
    coordinate: &BinlogCoordinate,
) -> RowResult<Option<TargetRowChange>> {
    primary_key_values(table, row, coordinate)?;
    Ok(Some(TargetRowChange {
        statement: build_delete_statement(table, row, coordinate)?,
        kind: TargetRowChangeKind::Delete,
        schema: table.schema.clone(),
        table: table.table.clone(),
        values: row.clone(),
    }))
}

impl<E> TargetExecutor for &E
where
    E: TargetExecutor,
{
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        (*self).execute(statement)
    }

    fn execute_row_change(&self, change: &TargetRowChange) -> Result<(), TargetExecuteError> {
        (*self).execute_row_change(change)
    }
}
