use super::*;

pub(super) fn apply_table_map_event<E, R>(
    applier: &mut RowApplier<E>,
    schema_resolver: &R,
    state: &mut StructuredEventState,
    coordinate: &BinlogCoordinate,
    table_map: &MysqlCdcTableMapEvent,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
    R: TableSchemaResolver,
{
    if !state.should_apply_schema(&table_map.database_name) {
        state.ignore_table_id(table_map.table_id);
        return Ok(EventPolicy::Ignore);
    }

    state.apply_table_id(table_map.table_id);
    // A table whose event shape cannot be mapped is ignored for as long as this table map stands,
    // so its row events are skipped instead of stopping the stream. A later full data sync supplies
    // those rows; the skip is logged per table map with both column counts.
    let Some(event) = map_table_map_event(coordinate, table_map, schema_resolver)? else {
        state.ignore_table_id(table_map.table_id);
        return Ok(EventPolicy::Ignore);
    };
    applier.apply_table_map(event);
    Ok(EventPolicy::ApplyTableMap)
}

pub(super) fn apply_write_rows_event<E>(
    applier: &mut RowApplier<E>,
    state: &StructuredEventState,
    coordinate: &BinlogCoordinate,
    rows: &mysql_cdc::events::row_events::write_rows_event::WriteRowsEvent,
    conflict_context: Option<&mut RowConflictContext<'_>>,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
{
    if state.is_ignored_table_id(rows.table_id) {
        return Ok(EventPolicy::Ignore);
    }
    require_full_row_image(&rows.columns_present, "write")?;
    let table = row_event_table_map(applier, rows.table_id, coordinate)?;
    let event = crate::row::WriteRowsEvent {
        coordinate: coordinate.clone(),
        table_id: rows.table_id,
        rows: map_row_data_list(&rows.rows, &table)?,
    };
    if let Some(context) = conflict_context {
        applier
            .apply_write_rows_with_conflicts(&event, context)
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    } else {
        applier
            .apply_write_rows(&event)
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    }
    Ok(EventPolicy::ApplyRows)
}

pub(super) fn apply_update_rows_event<E>(
    applier: &mut RowApplier<E>,
    state: &StructuredEventState,
    coordinate: &BinlogCoordinate,
    rows: &mysql_cdc::events::row_events::update_rows_event::UpdateRowsEvent,
    conflict_context: Option<&mut RowConflictContext<'_>>,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
{
    if state.is_ignored_table_id(rows.table_id) {
        return Ok(EventPolicy::Ignore);
    }
    require_full_row_image(&rows.columns_before_update, "update before")?;
    require_full_row_image(&rows.columns_after_update, "update after")?;
    let table = row_event_table_map(applier, rows.table_id, coordinate)?;
    let event = crate::row::UpdateRowsEvent {
        coordinate: coordinate.clone(),
        table_id: rows.table_id,
        rows: rows
            .rows
            .iter()
            .map(|row| map_update_row_data(row, &table))
            .collect::<Result<Vec<_>, _>>()?,
    };
    if let Some(context) = conflict_context {
        applier
            .apply_update_rows_with_conflicts(&event, context)
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    } else {
        applier
            .apply_update_rows(&event)
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    }
    Ok(EventPolicy::ApplyRows)
}

pub(super) fn apply_delete_rows_event<E>(
    applier: &mut RowApplier<E>,
    state: &StructuredEventState,
    coordinate: &BinlogCoordinate,
    rows: &mysql_cdc::events::row_events::delete_rows_event::DeleteRowsEvent,
    conflict_context: Option<&mut RowConflictContext<'_>>,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
{
    if state.is_ignored_table_id(rows.table_id) {
        return Ok(EventPolicy::Ignore);
    }
    require_full_row_image(&rows.columns_present, "delete")?;
    let table = row_event_table_map(applier, rows.table_id, coordinate)?;
    let event = DeleteRowsEvent {
        coordinate: coordinate.clone(),
        table_id: rows.table_id,
        rows: map_row_data_list(&rows.rows, &table)?,
    };
    if let Some(context) = conflict_context {
        applier
            .apply_delete_rows_with_conflicts(&event, context)
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    } else {
        applier
            .apply_delete_rows(&event)
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    }
    Ok(EventPolicy::ApplyRows)
}

pub(super) fn row_event_table_map<E>(
    applier: &RowApplier<E>,
    table_id: u64,
    coordinate: &BinlogCoordinate,
) -> Result<RowTableMap, ApplyBinlogError>
where
    E: TargetExecutor,
{
    applier.table_map(table_id).cloned().ok_or_else(|| {
        mapping_error(format!(
            "missing table map for table id {table_id} at {}:{}",
            coordinate.file, coordinate.position
        ))
    })
}

pub(super) fn map_update_row_data(
    row: &mysql_cdc::events::row_events::row_data::UpdateRowData,
    table: &RowTableMap,
) -> Result<RowUpdate, ApplyBinlogError> {
    Ok(RowUpdate {
        before: map_row_data(&row.before_update, table)?,
        after: map_row_data(&row.after_update, table)?,
    })
}

pub(super) fn map_row_data_list(
    rows: &[RowData],
    table: &RowTableMap,
) -> Result<Vec<RowImage>, ApplyBinlogError> {
    rows.iter().map(|row| map_row_data(row, table)).collect()
}

pub(super) fn map_row_data(
    row: &RowData,
    table: &RowTableMap,
) -> Result<RowImage, ApplyBinlogError> {
    if row.cells.len() != table.columns.len() {
        return Err(mapping_error(format!(
            "row has {} cells but table map has {} columns",
            row.cells.len(),
            table.columns.len()
        )));
    }

    table
        .columns
        .iter()
        .zip(&row.cells)
        .map(|(column, value)| {
            let signed = table.signed_columns.contains(column);
            mysql_value_to_target_value(value, signed, table.enum_columns.get(column))
                .map(|target_value| (column.clone(), target_value))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()
}
