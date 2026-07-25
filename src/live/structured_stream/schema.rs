use super::*;
use crate::inventory::TableInventory;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ResolvedTableSchema {
    pub(super) columns: Vec<String>,
    pub(super) primary_key: Vec<String>,
    pub(super) generated_columns: Vec<String>,
    pub(super) signed_columns: Vec<String>,
    pub(super) enum_columns: BTreeMap<String, Vec<String>>,
    pub(super) set_columns: BTreeMap<String, Vec<String>>,
}

pub(super) trait TableSchemaResolver {
    fn resolve_table_schema(
        &self,
        schema: &str,
        table: &str,
        column_count: usize,
    ) -> Result<ResolvedTableSchema, ApplyBinlogError>;

    fn invalidate_schema(&self, _schema: &str) {}
}

pub(super) struct TargetInventorySchemaResolver {
    reader: MariaDbInventoryReader,
    source_database: Option<String>,
    target_database: String,
    inventories: RefCell<BTreeMap<String, SchemaInventory>>,
}

impl TargetInventorySchemaResolver {
    pub(super) fn new(config: &ApplyBinlogConfig) -> Self {
        Self {
            reader: MariaDbInventoryReader::new(target_inventory_config(config)),
            source_database: config.source.database.clone(),
            target_database: config.target.database.clone(),
            inventories: RefCell::new(BTreeMap::new()),
        }
    }

    pub(super) fn target_schema_name(&self, schema: &str) -> String {
        if self.source_database.as_deref() == Some(schema) {
            return self.target_database.clone();
        }
        schema.to_string()
    }
}

impl TableSchemaResolver for TargetInventorySchemaResolver {
    fn resolve_table_schema(
        &self,
        schema: &str,
        table: &str,
        column_count: usize,
    ) -> Result<ResolvedTableSchema, ApplyBinlogError> {
        self.ensure_schema_inventory(schema)?;
        let inventories = self.inventories.borrow();
        let inventory = inventories.get(schema).ok_or_else(|| {
            mapping_error(format!("source schema {schema} inventory was not cached"))
        })?;
        let table = inventory
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| mapping_error(format!("source table {schema}.{table} was not found")))?;
        build_inventory_table_schema(schema, table, column_count)
    }

    fn invalidate_schema(&self, schema: &str) {
        self.inventories.borrow_mut().remove(schema);
    }
}

impl TargetInventorySchemaResolver {
    fn ensure_schema_inventory(&self, schema: &str) -> Result<(), ApplyBinlogError> {
        if self.inventories.borrow().contains_key(schema) {
            return Ok(());
        }

        let target_schema = self.target_schema_name(schema);
        let inventory = build_inventory(&target_schema, &self.reader).map_err(|error| {
            mapping_error(format!(
                "failed to read target schema {target_schema}: {error}"
            ))
        })?;
        self.inventories
            .borrow_mut()
            .insert(schema.to_string(), inventory);
        Ok(())
    }
}

pub(super) fn resolve_table_schema<R>(
    table_map: &MysqlCdcTableMapEvent,
    schema_resolver: &R,
) -> Result<ResolvedTableSchema, ApplyBinlogError>
where
    R: TableSchemaResolver,
{
    let column_count = table_map.column_types.len();
    let fallback = || fallback_table_schema(table_map, schema_resolver, column_count);
    let Some(metadata) = table_map.table_metadata.as_ref() else {
        return fallback();
    };
    let Some(columns) = validated_metadata_columns(table_map, metadata, column_count)? else {
        return fallback();
    };
    let fallback_schema = fallback_schema_for_metadata(metadata, &fallback)?;
    let primary_key = metadata_primary_key(metadata, &columns, &fallback_schema)?;
    let enum_columns = metadata_enum_columns(table_map, metadata, &columns, &fallback_schema)?;
    let set_columns = metadata_set_columns(table_map, metadata, &columns, &fallback_schema)?;
    Ok(resolved_table_schema(
        columns,
        primary_key,
        enum_columns,
        set_columns,
        fallback_schema,
    ))
}

fn validated_metadata_columns(
    table_map: &MysqlCdcTableMapEvent,
    metadata: &TableMetadata,
    column_count: usize,
) -> Result<Option<Vec<String>>, ApplyBinlogError> {
    let Some(columns) = metadata.column_names.clone() else {
        return Ok(None);
    };
    validate_column_count(
        &table_map.database_name,
        &table_map.table_name,
        column_count,
        &columns,
    )?;
    Ok(Some(columns))
}

fn resolved_table_schema(
    columns: Vec<String>,
    primary_key: Vec<String>,
    enum_columns: BTreeMap<String, Vec<String>>,
    set_columns: BTreeMap<String, Vec<String>>,
    fallback: Option<ResolvedTableSchema>,
) -> ResolvedTableSchema {
    let (generated_columns, signed_columns) = fallback
        .as_ref()
        .map(|schema| {
            (
                schema.generated_columns.clone(),
                schema.signed_columns.clone(),
            )
        })
        .unwrap_or_default();
    ResolvedTableSchema {
        columns,
        primary_key,
        generated_columns,
        signed_columns,
        enum_columns,
        set_columns,
    }
}

fn build_inventory_table_schema(
    schema: &str,
    table: &TableInventory,
    column_count: usize,
) -> Result<ResolvedTableSchema, ApplyBinlogError> {
    let columns: Vec<String> = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    // Deliberately NOT tolerant of a count mismatch. Taking the leading `column_count` target
    // columns only aligns when the missing columns were appended. This source inserts columns
    // mid-table - home_feed_rss_articles gained image_width/image_height at positions 7 and 8 -
    // so the leading slice shifts every later value into the wrong column. MySQL caught one such
    // write only because an excerpt landed in an integer column; where the types line up it would
    // corrupt silently. Fail closed and let the count check below reject the event.
    let generated_columns = generated_column_names(table);
    let signed_columns = signed_column_names(table);
    let enum_columns = enum_column_values(table);
    let set_columns = set_column_values(table);
    validate_column_count(schema, &table.name, column_count, &columns)?;
    Ok(ResolvedTableSchema {
        columns,
        primary_key: table.primary_key.clone(),
        generated_columns,
        signed_columns,
        enum_columns,
        set_columns,
    })
}

fn generated_column_names(table: &TableInventory) -> Vec<String> {
    table
        .columns
        .iter()
        .filter(|column| column.generated.is_some())
        .map(|column| column.name.clone())
        .collect()
}

fn signed_column_names(table: &TableInventory) -> Vec<String> {
    table
        .columns
        .iter()
        .filter(|column| is_signed_integer_column(&column.data_type, &column.column_type))
        .map(|column| column.name.clone())
        .collect()
}

fn enum_column_values(table: &TableInventory) -> BTreeMap<String, Vec<String>> {
    table
        .columns
        .iter()
        .filter_map(|column| {
            parse_enum_column_type(&column.column_type).map(|values| (column.name.clone(), values))
        })
        .collect()
}

fn set_column_values(table: &TableInventory) -> BTreeMap<String, Vec<String>> {
    table
        .columns
        .iter()
        .filter_map(|column| {
            parse_set_column_type(&column.column_type).map(|values| (column.name.clone(), values))
        })
        .collect()
}

fn fallback_table_schema<R>(
    table_map: &MysqlCdcTableMapEvent,
    resolver: &R,
    column_count: usize,
) -> Result<ResolvedTableSchema, ApplyBinlogError>
where
    R: TableSchemaResolver,
{
    resolver.resolve_table_schema(
        &table_map.database_name,
        &table_map.table_name,
        column_count,
    )
}

fn fallback_schema_for_metadata(
    metadata: &TableMetadata,
    fallback: &impl Fn() -> Result<ResolvedTableSchema, ApplyBinlogError>,
) -> Result<Option<ResolvedTableSchema>, ApplyBinlogError> {
    if metadata.simple_primary_keys.is_some() {
        return Ok(fallback().ok());
    }
    fallback().map(Some)
}

fn metadata_primary_key(
    metadata: &TableMetadata,
    columns: &[String],
    fallback: &Option<ResolvedTableSchema>,
) -> Result<Vec<String>, ApplyBinlogError> {
    match primary_key_from_metadata(metadata, columns)? {
        Some(primary_key) => Ok(primary_key),
        None => Ok(fallback
            .as_ref()
            .expect("fallback schema exists when metadata lacks primary key")
            .primary_key
            .clone()),
    }
}

fn metadata_enum_columns(
    table_map: &MysqlCdcTableMapEvent,
    metadata: &TableMetadata,
    columns: &[String],
    fallback: &Option<ResolvedTableSchema>,
) -> Result<BTreeMap<String, Vec<String>>, ApplyBinlogError> {
    let metadata_values = enum_columns_from_metadata(table_map, metadata, columns)?;
    if metadata_values.is_empty() {
        return Ok(fallback
            .as_ref()
            .map(|schema| schema.enum_columns.clone())
            .unwrap_or_default());
    }
    Ok(metadata_values)
}

fn metadata_set_columns(
    table_map: &MysqlCdcTableMapEvent,
    metadata: &TableMetadata,
    columns: &[String],
    fallback: &Option<ResolvedTableSchema>,
) -> Result<BTreeMap<String, Vec<String>>, ApplyBinlogError> {
    let metadata_values = set_columns_from_metadata(table_map, metadata, columns)?;
    if metadata_values.is_empty() {
        return Ok(fallback
            .as_ref()
            .map(|schema| schema.set_columns.clone())
            .unwrap_or_default());
    }
    Ok(metadata_values)
}

pub(super) fn map_table_map_event<R>(
    coordinate: &BinlogCoordinate,
    table_map: &MysqlCdcTableMapEvent,
    schema_resolver: &R,
) -> Result<TableMapEvent, ApplyBinlogError>
where
    R: TableSchemaResolver,
{
    let schema = resolve_table_schema(table_map, schema_resolver)?;
    Ok(TableMapEvent {
        coordinate: coordinate.clone(),
        table: RowTableMap {
            table_id: table_map.table_id,
            schema: table_map.database_name.clone(),
            table: table_map.table_name.clone(),
            columns: schema.columns,
            primary_key: schema.primary_key,
            generated_columns: schema.generated_columns,
            signed_columns: schema.signed_columns,
            enum_columns: schema.enum_columns,
            set_columns: schema.set_columns,
        },
    })
}

pub(super) fn enum_columns_from_metadata(
    table_map: &MysqlCdcTableMapEvent,
    metadata: &TableMetadata,
    columns: &[String],
) -> Result<BTreeMap<String, Vec<String>>, ApplyBinlogError> {
    let Some(enum_value_sets) = &metadata.enum_string_values else {
        return Ok(BTreeMap::new());
    };
    let enum_column_indexes = enum_column_indexes(table_map);
    if enum_column_indexes.len() != enum_value_sets.len() {
        return Err(mapping_error(format!(
            "table map enum metadata has {} enum columns but {} enum value sets",
            enum_column_indexes.len(),
            enum_value_sets.len()
        )));
    }

    enum_column_indexes
        .into_iter()
        .zip(enum_value_sets.iter())
        .map(|(column_index, values)| {
            let column = columns.get(column_index).cloned().ok_or_else(|| {
                mapping_error(format!("enum column index {column_index} is out of range"))
            })?;
            Ok((column, values.clone()))
        })
        .collect()
}

fn enum_column_indexes(table_map: &MysqlCdcTableMapEvent) -> Vec<usize> {
    table_map
        .column_types
        .iter()
        .enumerate()
        .filter_map(|(index, column_type)| {
            (*column_type == MYSQL_COLUMN_TYPE_ENUM).then_some(index)
        })
        .collect()
}

pub(super) fn set_columns_from_metadata(
    table_map: &MysqlCdcTableMapEvent,
    metadata: &TableMetadata,
    columns: &[String],
) -> Result<BTreeMap<String, Vec<String>>, ApplyBinlogError> {
    let Some(set_value_sets) = &metadata.set_string_values else {
        return Ok(BTreeMap::new());
    };
    let set_column_indexes = table_map
        .column_types
        .iter()
        .zip(&table_map.column_metadata)
        .enumerate()
        .filter_map(|(index, (column_type, metadata))| {
            is_set_column(*column_type, *metadata).then_some(index)
        })
        .collect::<Vec<_>>();
    if set_column_indexes.len() != set_value_sets.len() {
        return Err(mapping_error(format!(
            "table map SET metadata has {} SET columns but {} SET value sets",
            set_column_indexes.len(),
            set_value_sets.len()
        )));
    }

    set_column_indexes
        .into_iter()
        .zip(set_value_sets.iter())
        .map(|(column_index, values)| {
            let column = columns.get(column_index).cloned().ok_or_else(|| {
                mapping_error(format!("SET column index {column_index} is out of range"))
            })?;
            Ok((column, values.clone()))
        })
        .collect()
}

fn is_set_column(column_type: u8, metadata: u16) -> bool {
    column_type == MYSQL_COLUMN_TYPE_SET
        || (column_type == MYSQL_COLUMN_TYPE_STRING
            && metadata >> 8 == u16::from(MYSQL_COLUMN_TYPE_SET))
}

pub(super) fn parse_enum_column_type(column_type: &str) -> Option<Vec<String>> {
    let values = column_type.strip_prefix("enum(")?.strip_suffix(')')?;
    Some(parse_sql_string_list(values))
}

pub(super) fn parse_set_column_type(column_type: &str) -> Option<Vec<String>> {
    let values = column_type.strip_prefix("set(")?.strip_suffix(')')?;
    Some(parse_sql_string_list(values))
}

pub(super) fn parse_sql_string_list(values: &str) -> Vec<String> {
    let mut parsed = Vec::new();
    let mut chars = values.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\'' {
            parsed.push(parse_sql_string_value(&mut chars));
        }
    }
    parsed
}

pub(super) fn parse_sql_string_value<I>(chars: &mut std::iter::Peekable<I>) -> String
where
    I: Iterator<Item = char>,
{
    let mut value = String::new();
    while let Some(character) = chars.next() {
        match character {
            '\'' if chars.peek() == Some(&'\'') => {
                value.push('\'');
                chars.next();
            }
            '\'' => break,
            '\\' => {
                if let Some(escaped) = chars.next() {
                    value.push(escaped);
                }
            }
            _ => value.push(character),
        }
    }
    value
}

pub(super) fn is_signed_integer_column(data_type: &str, column_type: &str) -> bool {
    matches!(
        data_type,
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
    ) && !column_type.to_ascii_lowercase().contains("unsigned")
}

pub(super) fn primary_key_from_metadata(
    metadata: &TableMetadata,
    columns: &[String],
) -> Result<Option<Vec<String>>, ApplyBinlogError> {
    let Some(primary_key_indexes) = &metadata.simple_primary_keys else {
        return Ok(None);
    };
    primary_key_indexes
        .iter()
        .map(|index| primary_key_column(*index, columns))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub(super) fn primary_key_column(
    index: u32,
    columns: &[String],
) -> Result<String, ApplyBinlogError> {
    columns
        .get(index as usize)
        .cloned()
        .ok_or_else(|| mapping_error(format!("primary key column index {index} is out of range")))
}

pub(super) fn validate_column_count(
    schema: &str,
    table: &str,
    expected: usize,
    columns: &[String],
) -> Result<(), ApplyBinlogError> {
    if columns.len() == expected {
        return Ok(());
    }

    Err(mapping_error(format!(
        "schema for {schema}.{table} has {} columns but row event table map has {expected}",
        columns.len()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::{ColumnInventory, TableInventory};

    fn column(name: &str) -> ColumnInventory {
        ColumnInventory {
            name: name.to_string(),
            ordinal_position: 0,
            column_type: "bigint".to_string(),
            data_type: "bigint".to_string(),
            is_nullable: true,
            character_set: None,
            collation: None,
            default_value: None,
            extra: String::new(),
            comment: String::new(),
            generated: None,
        }
    }

    fn table(names: &[&str]) -> TableInventory {
        TableInventory {
            name: "users_profiles".to_string(),
            table_type: "BASE TABLE".to_string(),
            engine: Some("InnoDB".to_string()),
            collation: Some("utf8mb4_0900_ai_ci".to_string()),
            primary_key: vec!["user_id".to_string()],
            columns: names.iter().map(|name| column(name)).collect(),
        }
    }

    /// A count mismatch must fail closed in BOTH directions. Mapping the event onto the leading
    /// target columns silently shifts every value when the absent columns were inserted mid-table,
    /// which this source does.
    #[test]
    fn a_target_with_extra_columns_fails_rather_than_shifting_values() {
        let converged = table(&[
            "user_id",
            "bio",
            "app_next_eligible",
            "app_next_opted_out_at",
        ]);

        let error = build_inventory_table_schema("globalcomix", &converged, 2)
            .expect_err("a count mismatch cannot be resolved by position");

        assert!(
            error
                .to_string()
                .contains("has 4 columns but row event table map has 2"),
            "{error}"
        );
    }

    /// The opposite direction has nowhere to put the event's values and must still fail.
    #[test]
    fn a_target_missing_columns_the_event_carries_still_fails() {
        let behind = table(&["user_id", "bio"]);

        let error = build_inventory_table_schema("globalcomix", &behind, 4)
            .expect_err("a target with fewer columns cannot map the event");

        assert!(
            error
                .to_string()
                .contains("has 2 columns but row event table map has 4"),
            "{error}"
        );
    }
}
