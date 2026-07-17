use super::model::{
    ColumnInventory, ColumnRow, EventInventory, EventRow, ForeignKeyInventory, ForeignKeyRow,
    GeneratedColumn, IndexColumnInventory, IndexInventory, IndexRow, InventoryError,
    InventoryReader, PrimaryKeyRow, RoutineInventory, RoutineRow, SchemaInventory, TableInventory,
    TableRow, TriggerInventory, TriggerRow, ViewInventory, ViewRow,
};
use crate::conflict_repair::{CanonicalForeignKey, canonicalize_foreign_keys};
use std::collections::BTreeMap;

const BASE_TABLE_TYPE: &str = "BASE TABLE";

pub fn build_canonical_foreign_key_inventory(
    schema: &str,
    reader: &impl InventoryReader,
) -> Result<Vec<CanonicalForeignKey>, InventoryError> {
    canonicalize_foreign_keys(reader.read_canonical_foreign_keys(schema)?)
        .map_err(InventoryError::new)
}

pub fn build_inventory(
    schema: &str,
    reader: &impl InventoryReader,
) -> Result<SchemaInventory, InventoryError> {
    let tables = reader.read_tables(schema)?;
    let columns = group_columns(reader.read_columns(schema)?);
    let primary_keys = group_primary_keys(reader.read_primary_keys(schema)?);
    let indexes = build_indexes(reader.read_indexes(schema)?)?;
    let foreign_keys = build_foreign_keys(reader.read_foreign_keys(schema)?);

    Ok(SchemaInventory {
        schema: schema.to_string(),
        tables: build_tables(tables, columns, primary_keys),
        indexes,
        foreign_keys,
        views: build_views(reader.read_views(schema)?),
        triggers: build_triggers(reader.read_triggers(schema)?),
        routines: build_routines(reader.read_routines(schema)?),
        events: build_events(reader.read_events(schema)?),
    })
}

pub(crate) fn build_tables(
    table_rows: Vec<TableRow>,
    columns: BTreeMap<String, Vec<ColumnInventory>>,
    primary_keys: BTreeMap<String, Vec<String>>,
) -> Vec<TableInventory> {
    table_rows
        .into_iter()
        .filter(|row| row.table_type == BASE_TABLE_TYPE)
        .map(|row| {
            let table_columns = columns.get(&row.table_name).cloned().unwrap_or_default();
            let primary_key = primary_keys
                .get(&row.table_name)
                .cloned()
                .unwrap_or_default();

            TableInventory {
                name: row.table_name,
                table_type: row.table_type,
                engine: row.engine,
                collation: row.table_collation,
                primary_key,
                columns: table_columns,
            }
        })
        .collect()
}

pub(crate) fn group_columns(rows: Vec<ColumnRow>) -> BTreeMap<String, Vec<ColumnInventory>> {
    let mut columns_by_table: BTreeMap<String, Vec<ColumnInventory>> = BTreeMap::new();

    for row in rows {
        columns_by_table
            .entry(row.table_name.clone())
            .or_default()
            .push(build_column(row));
    }

    for columns in columns_by_table.values_mut() {
        columns.sort_by_key(|column| column.ordinal_position);
    }

    columns_by_table
}

pub(crate) fn build_column(row: ColumnRow) -> ColumnInventory {
    let generated = build_generated_column(&row);

    ColumnInventory {
        name: row.column_name,
        ordinal_position: row.ordinal_position,
        column_type: row.column_type,
        data_type: row.data_type,
        is_nullable: row.is_nullable,
        default_value: row.column_default,
        extra: row.extra.clone(),
        comment: row.column_comment,
        generated,
    }
}

pub(crate) fn build_generated_column(row: &ColumnRow) -> Option<GeneratedColumn> {
    let expression = row.generation_expression.as_ref()?;

    Some(GeneratedColumn {
        expression: expression.clone(),
        generation_kind: generation_kind(&row.extra).to_string(),
    })
}

pub(crate) fn generation_kind(extra: &str) -> &'static str {
    if extra.to_ascii_uppercase().contains("STORED") {
        "STORED"
    } else {
        "VIRTUAL"
    }
}

pub(crate) fn group_primary_keys(rows: Vec<PrimaryKeyRow>) -> BTreeMap<String, Vec<String>> {
    let mut rows_by_table: BTreeMap<String, Vec<PrimaryKeyRow>> = BTreeMap::new();

    for row in rows {
        rows_by_table
            .entry(row.table_name.clone())
            .or_default()
            .push(row);
    }

    rows_by_table
        .into_iter()
        .map(|(table, mut rows)| {
            rows.sort_by_key(|row| row.ordinal_position);
            let columns = rows.into_iter().map(|row| row.column_name).collect();
            (table, columns)
        })
        .collect()
}

pub(crate) fn build_foreign_keys(rows: Vec<ForeignKeyRow>) -> Vec<ForeignKeyInventory> {
    let mut grouped: BTreeMap<(String, String), Vec<ForeignKeyRow>> = BTreeMap::new();
    for row in rows {
        grouped
            .entry((row.table_name.clone(), row.constraint_name.clone()))
            .or_default()
            .push(row);
    }
    grouped
        .into_iter()
        .map(|((table, name), mut rows)| {
            rows.sort_by_key(|row| row.sequence);
            let referenced_table = rows
                .first()
                .map(|row| row.referenced_table.clone())
                .unwrap_or_default();
            ForeignKeyInventory {
                table,
                name,
                columns: rows.iter().map(|row| row.column_name.clone()).collect(),
                referenced_table,
                referenced_columns: rows.into_iter().map(|row| row.referenced_column).collect(),
            }
        })
        .collect()
}

pub(crate) fn build_indexes(rows: Vec<IndexRow>) -> Result<Vec<IndexInventory>, InventoryError> {
    group_index_rows(rows)
        .into_iter()
        .map(build_index)
        .collect()
}

fn group_index_rows(rows: Vec<IndexRow>) -> BTreeMap<(String, String), Vec<IndexRow>> {
    let mut grouped = BTreeMap::new();
    for row in rows {
        grouped
            .entry((row.table_name.clone(), row.index_name.clone()))
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn build_index(
    ((table, name), mut rows): ((String, String), Vec<IndexRow>),
) -> Result<IndexInventory, InventoryError> {
    rows.sort_by_key(|row| row.sequence);
    let metadata = index_metadata(&table, &name, &rows)?;
    let columns = build_index_columns(&table, &name, rows)?;

    Ok(IndexInventory {
        table,
        name,
        unique: !metadata.non_unique,
        index_type: metadata.index_type,
        visible: metadata.visible,
        comment: metadata.comment,
        columns,
    })
}

struct IndexMetadata {
    non_unique: bool,
    index_type: String,
    visible: bool,
    comment: Option<String>,
}

fn index_metadata(
    table: &str,
    name: &str,
    rows: &[IndexRow],
) -> Result<IndexMetadata, InventoryError> {
    let first = rows
        .first()
        .ok_or_else(|| InventoryError::new("empty grouped index"))?;
    let metadata = IndexMetadata {
        non_unique: first.non_unique,
        index_type: first.index_type.clone(),
        visible: first.visible,
        comment: first.comment.clone(),
    };
    validate_index_metadata(table, name, rows, &metadata)?;
    Ok(metadata)
}

fn validate_index_metadata(
    table: &str,
    name: &str,
    rows: &[IndexRow],
    metadata: &IndexMetadata,
) -> Result<(), InventoryError> {
    if rows
        .iter()
        .any(|row| row.non_unique != metadata.non_unique || row.index_type != metadata.index_type)
    {
        return Err(InventoryError::new(format!(
            "inconsistent index metadata for {table}.{name}"
        )));
    }
    Ok(())
}

fn build_index_columns(
    table: &str,
    name: &str,
    rows: Vec<IndexRow>,
) -> Result<Vec<IndexColumnInventory>, InventoryError> {
    rows.into_iter()
        .map(|row| build_index_column(table, name, row))
        .collect()
}

fn build_index_column(
    table: &str,
    name: &str,
    row: IndexRow,
) -> Result<IndexColumnInventory, InventoryError> {
    let column_name = row.column_name.ok_or_else(|| {
        InventoryError::new(format!(
            "functional index {table}.{name} lacks portable column metadata"
        ))
    })?;
    let order = if row.collation.as_deref() == Some("D") {
        "DESC".to_string()
    } else {
        "ASC".to_string()
    };
    Ok(IndexColumnInventory {
        name: column_name,
        sequence: row.sequence,
        prefix_length: row.prefix_length,
        collation: row.collation,
        order,
    })
}

pub(crate) fn build_views(rows: Vec<ViewRow>) -> Vec<ViewInventory> {
    rows.into_iter()
        .map(|row| ViewInventory {
            name: row.table_name,
            definition: row.view_definition,
        })
        .collect()
}

pub(crate) fn build_triggers(rows: Vec<TriggerRow>) -> Vec<TriggerInventory> {
    rows.into_iter()
        .map(|row| TriggerInventory {
            name: row.trigger_name,
            table: row.event_object_table,
            timing: row.action_timing,
            event: row.event_manipulation,
            statement: row.action_statement,
        })
        .collect()
}

pub(crate) fn build_routines(rows: Vec<RoutineRow>) -> Vec<RoutineInventory> {
    rows.into_iter()
        .map(|row| RoutineInventory {
            name: row.routine_name,
            routine_type: row.routine_type,
            definition: row.routine_definition,
        })
        .collect()
}

pub(crate) fn build_events(rows: Vec<EventRow>) -> Vec<EventInventory> {
    rows.into_iter()
        .map(|row| EventInventory {
            name: row.event_name,
            status: row.status,
            definition: row.event_definition,
        })
        .collect()
}
