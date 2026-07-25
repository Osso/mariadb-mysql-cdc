use super::model::{
    ColumnRow, EventRow, ForeignKeyRow, IndexRow, InventoryError, PrimaryKeyRow, RoutineRow,
    SchemaDefaults, SourceMasterCoordinate, TableRow, TableRuntimeMetadata, TriggerRow, ViewRow,
};
use crate::conflict_repair::CanonicalForeignKeyRow;

pub(crate) fn parse_schema_defaults(fields: &[String]) -> Result<SchemaDefaults, InventoryError> {
    require_len(fields, 2, "schema defaults")?;
    if fields[0].is_empty() || fields[1].is_empty() {
        return Err(InventoryError::new(
            "schema defaults require non-empty character set and collation",
        ));
    }
    Ok(SchemaDefaults {
        character_set: fields[0].clone(),
        collation: fields[1].clone(),
    })
}

pub(crate) fn parse_source_master_coordinate(
    fields: &[String],
) -> Result<SourceMasterCoordinate, InventoryError> {
    if fields.len() < 2 {
        return Err(InventoryError::new(format!(
            "source master coordinate row has {} fields, expected at least 2",
            fields.len()
        )));
    }
    Ok(SourceMasterCoordinate {
        file: fields[0].clone(),
        position: fields[1].parse().map_err(|_| {
            InventoryError::new(format!(
                "source master coordinate position is not numeric: {}",
                fields[1]
            ))
        })?,
    })
}

pub(crate) fn parse_table_runtime_row(
    fields: &[String],
) -> Result<TableRuntimeMetadata, InventoryError> {
    require_len(fields, 2, "table runtime")?;
    Ok(TableRuntimeMetadata {
        row_count: fields[0].parse().map_err(|_| {
            InventoryError::new(format!(
                "table runtime row count is not numeric: {}",
                fields[0]
            ))
        })?,
        auto_increment: optional_string(&fields[1])
            .map(|value| {
                value.parse().map_err(|_| {
                    InventoryError::new(format!(
                        "table runtime auto increment is not numeric: {value}"
                    ))
                })
            })
            .transpose()?,
    })
}

pub(crate) fn parse_table_row(fields: &[String]) -> Result<TableRow, InventoryError> {
    require_len(fields, 4, "table")?;

    Ok(TableRow {
        table_name: fields[0].clone(),
        table_type: fields[1].clone(),
        engine: optional_string(&fields[2]),
        table_collation: optional_string(&fields[3]),
    })
}

pub(crate) fn parse_column_row(fields: &[String]) -> Result<ColumnRow, InventoryError> {
    require_len(fields, 13, "column")?;

    Ok(ColumnRow {
        table_name: fields[0].clone(),
        column_name: fields[1].clone(),
        ordinal_position: parse_u32(&fields[2], "column ordinal")?,
        column_type: fields[3].clone(),
        data_type: fields[4].clone(),
        is_nullable: fields[5] == "YES",
        character_set: optional_string(&fields[6]),
        collation: optional_string(&fields[7]),
        // An empty string is a real default, so only a SQL NULL means the column has none.
        column_default: (fields[12] != "1").then(|| fields[8].clone()),
        extra: fields[9].clone(),
        column_comment: fields[10].clone(),
        generation_expression: optional_string(&fields[11]),
    })
}

pub(crate) fn parse_primary_key_row(fields: &[String]) -> Result<PrimaryKeyRow, InventoryError> {
    require_len(fields, 3, "primary key")?;

    Ok(PrimaryKeyRow {
        table_name: fields[0].clone(),
        column_name: fields[1].clone(),
        ordinal_position: parse_u32(&fields[2], "primary key ordinal")?,
    })
}

pub(crate) fn parse_index_row(fields: &[String]) -> Result<IndexRow, InventoryError> {
    require_len(fields, 10, "index")?;
    let column_name = optional_string(&fields[5]);
    if column_name.is_none() {
        return Err(InventoryError::new(format!(
            "functional index {}.{} lacks portable column metadata",
            fields[0], fields[1]
        )));
    }
    Ok(IndexRow {
        table_name: fields[0].clone(),
        index_name: fields[1].clone(),
        non_unique: fields[2] != "0",
        index_type: fields[3].clone(),
        sequence: parse_u32(&fields[4], "index sequence")?,
        column_name,
        prefix_length: optional_u32(&fields[6], "index prefix length")?,
        collation: optional_string(&fields[7]),
        visible: fields[8].eq_ignore_ascii_case("YES"),
        comment: optional_string(&fields[9]),
    })
}

pub(crate) fn parse_foreign_key_row(fields: &[String]) -> Result<ForeignKeyRow, InventoryError> {
    require_len(fields, 7, "foreign key")?;
    Ok(ForeignKeyRow {
        table_name: fields[0].clone(),
        constraint_name: fields[1].clone(),
        column_name: fields[2].clone(),
        sequence: parse_u32(&fields[3], "foreign key sequence")?,
        referenced_schema: fields[4].clone(),
        referenced_table: fields[5].clone(),
        referenced_column: fields[6].clone(),
    })
}

pub(crate) fn parse_canonical_foreign_key_row(
    fields: &[String],
) -> Result<CanonicalForeignKeyRow, InventoryError> {
    require_len(fields, 13, "canonical foreign key")?;
    Ok(CanonicalForeignKeyRow {
        constraint_schema: fields[0].clone(),
        constraint_name: fields[1].clone(),
        child_schema: fields[2].clone(),
        child_table: fields[3].clone(),
        child_column: fields[4].clone(),
        ordinal_position: parse_u32(&fields[5], "canonical foreign key ordinal")?,
        parent_schema: fields[6].clone(),
        parent_table: fields[7].clone(),
        parent_column: fields[8].clone(),
        update_rule: fields[9].clone(),
        delete_rule: fields[10].clone(),
        match_option: fields[11].clone(),
        enforced: !fields[12].eq_ignore_ascii_case("NO"),
    })
}

pub(crate) fn parse_view_row(fields: &[String]) -> Result<ViewRow, InventoryError> {
    require_len(fields, 2, "view")?;

    Ok(ViewRow {
        table_name: fields[0].clone(),
        view_definition: fields[1].clone(),
    })
}

pub(crate) fn parse_trigger_row(fields: &[String]) -> Result<TriggerRow, InventoryError> {
    require_len(fields, 5, "trigger")?;

    Ok(TriggerRow {
        trigger_name: fields[0].clone(),
        event_manipulation: fields[1].clone(),
        action_timing: fields[2].clone(),
        event_object_table: fields[3].clone(),
        action_statement: fields[4].clone(),
    })
}

pub(crate) fn parse_routine_row(fields: &[String]) -> Result<RoutineRow, InventoryError> {
    require_len(fields, 3, "routine")?;

    Ok(RoutineRow {
        routine_name: fields[0].clone(),
        routine_type: fields[1].clone(),
        routine_definition: optional_string(&fields[2]),
    })
}

pub(crate) fn parse_event_row(fields: &[String]) -> Result<EventRow, InventoryError> {
    require_len(fields, 3, "event")?;

    Ok(EventRow {
        event_name: fields[0].clone(),
        status: fields[1].clone(),
        event_definition: fields[2].clone(),
    })
}

pub(crate) fn require_len(
    fields: &[String],
    expected: usize,
    row_type: &str,
) -> Result<(), InventoryError> {
    if fields.len() == expected {
        Ok(())
    } else {
        Err(InventoryError::new(format!(
            "{row_type} row has {} fields, expected {expected}",
            fields.len()
        )))
    }
}

pub(crate) fn optional_string(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

pub(crate) fn parse_u32(value: &str, field_name: &str) -> Result<u32, InventoryError> {
    value
        .parse()
        .map_err(|_| InventoryError::new(format!("{field_name} is not numeric: {value}")))
}

pub(crate) fn optional_u32(value: &str, field_name: &str) -> Result<Option<u32>, InventoryError> {
    optional_string(value)
        .map(|value| parse_u32(&value, field_name))
        .transpose()
}
