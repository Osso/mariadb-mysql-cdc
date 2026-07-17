use crate::mysql_support::quote_ident;

pub(crate) fn source_master_coordinate_query() -> &'static str {
    "SHOW MASTER STATUS"
}

pub(crate) fn schema_defaults_query(schema: &str) -> String {
    format!(
        "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = {}",
        quote_sql_string(schema)
    )
}

pub(crate) fn table_runtime_query(schema: &str, table: &str) -> String {
    format!(
        "SELECT (SELECT COUNT(*) FROM {}.{}), AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} AND TABLE_TYPE = 'BASE TABLE'",
        quote_ident(schema),
        quote_ident(table),
        quote_sql_string(schema),
        quote_sql_string(table),
    )
}

pub(crate) fn tables_query(schema: &str) -> String {
    format!(
        "SELECT TABLE_NAME, TABLE_TYPE, ENGINE, TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} ORDER BY TABLE_NAME",
        quote_sql_string(schema)
    )
}

pub(crate) fn columns_query(schema: &str) -> String {
    format!(
        "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT, GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {} ORDER BY TABLE_NAME, ORDINAL_POSITION",
        quote_sql_string(schema)
    )
}

pub(crate) fn primary_keys_query(schema: &str) -> String {
    format!(
        "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = {} AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY TABLE_NAME, ORDINAL_POSITION",
        quote_sql_string(schema)
    )
}

pub(crate) fn indexes_query(schema: &str) -> String {
    format!(
        "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, INDEX_TYPE, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART, COLLATION, 'YES' AS IS_VISIBLE, INDEX_COMMENT FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = {} AND INDEX_NAME <> 'PRIMARY' ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
        quote_sql_string(schema)
    )
}

pub(crate) fn foreign_keys_query(schema: &str) -> String {
    format!(
        "SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = {} AND REFERENCED_TABLE_NAME IS NOT NULL ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION",
        quote_sql_string(schema)
    )
}

pub(crate) fn canonical_foreign_keys_query(schema: &str) -> String {
    format!(
        "SELECT k.CONSTRAINT_SCHEMA, k.CONSTRAINT_NAME, k.TABLE_SCHEMA, k.TABLE_NAME, k.COLUMN_NAME, k.ORDINAL_POSITION, k.REFERENCED_TABLE_SCHEMA, k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME, COALESCE(r.UPDATE_RULE, 'RESTRICT'), COALESCE(r.DELETE_RULE, 'RESTRICT'), COALESCE(r.MATCH_OPTION, 'NONE'), 'YES' FROM information_schema.KEY_COLUMN_USAGE k LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS r ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND r.TABLE_NAME = k.TABLE_NAME WHERE k.TABLE_SCHEMA = {} AND k.REFERENCED_TABLE_NAME IS NOT NULL ORDER BY k.CONSTRAINT_SCHEMA, k.CONSTRAINT_NAME, k.ORDINAL_POSITION",
        quote_sql_string(schema)
    )
}

pub(crate) fn views_query(schema: &str) -> String {
    format!(
        "SELECT TABLE_NAME, VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA = {} ORDER BY TABLE_NAME",
        quote_sql_string(schema)
    )
}

pub(crate) fn triggers_query(schema: &str) -> String {
    format!(
        "SELECT TRIGGER_NAME, EVENT_MANIPULATION, ACTION_TIMING, EVENT_OBJECT_TABLE, ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = {} ORDER BY TRIGGER_NAME",
        quote_sql_string(schema)
    )
}

pub(crate) fn routines_query(schema: &str) -> String {
    format!(
        "SELECT ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = {} ORDER BY ROUTINE_NAME",
        quote_sql_string(schema)
    )
}

pub(crate) fn events_query(schema: &str) -> String {
    format!(
        "SELECT EVENT_NAME, STATUS, EVENT_DEFINITION FROM information_schema.EVENTS WHERE EVENT_SCHEMA = {} ORDER BY EVENT_NAME",
        quote_sql_string(schema)
    )
}

pub(crate) fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}
