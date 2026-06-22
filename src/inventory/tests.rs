use super::*;

#[test]
fn builds_inventory_with_primary_keys_and_generated_columns() {
    let reader = fake_reader();

    let inventory = build_inventory("fixture_cdc", &reader).expect("inventory");

    assert_eq!(inventory.schema, "fixture_cdc");
    assert_eq!(inventory.tables.len(), 1);

    let table = &inventory.tables[0];
    assert_eq!(table.name, "accounts");
    assert_eq!(table.primary_key, vec!["id"]);
    assert_eq!(table.columns[0].name, "id");
    assert_eq!(table.columns[0].data_type, "int");
    assert_eq!(table.columns[1].name, "name");
    assert!(!table.columns[1].is_nullable);
    assert_eq!(table.columns[2].generated, Some(generated_balance_column()));
}

#[test]
fn includes_views_triggers_routines_and_events() {
    let reader = fake_reader();

    let inventory = build_inventory("fixture_cdc", &reader).expect("inventory");

    assert_eq!(inventory.views[0].name, "account_balances");
    assert_eq!(
        inventory.views[0].definition,
        "select id, balance from accounts"
    );
    assert_eq!(inventory.triggers[0].name, "accounts_ai");
    assert_eq!(inventory.triggers[0].timing, "AFTER");
    assert_eq!(inventory.triggers[0].event, "INSERT");
    assert_eq!(inventory.routines[0].name, "recalculate_accounts");
    assert_eq!(inventory.routines[0].routine_type, "PROCEDURE");
    assert_eq!(inventory.events[0].name, "nightly_recalc");
    assert_eq!(inventory.events[0].status, "ENABLED");
}

#[test]
fn parses_cli_rows_and_quotes_schema_names() {
    let rows = parse_tsv("accounts\tBASE TABLE\tInnoDB\tutf8mb4_unicode_ci\n");
    let table = parse_table_row(&rows[0]).expect("table row");

    assert_eq!(table.table_name, "accounts");
    assert_eq!(table.engine, Some("InnoDB".to_string()));
    assert_eq!(quote_sql_string("app's\\schema"), "'app''s\\\\schema'");
}

#[test]
fn builds_information_schema_queries_with_quoted_schema() {
    let schema = "app's\\schema";
    let quoted = "'app''s\\\\schema'";

    assert!(tables_query(schema).contains(&format!("TABLE_SCHEMA = {quoted}")));
    assert!(columns_query(schema).contains(&format!("TABLE_SCHEMA = {quoted}")));
    assert!(primary_keys_query(schema).contains(&format!("TABLE_SCHEMA = {quoted}")));
    assert!(views_query(schema).contains(&format!("TABLE_SCHEMA = {quoted}")));
    assert!(triggers_query(schema).contains(&format!("TRIGGER_SCHEMA = {quoted}")));
    assert!(routines_query(schema).contains(&format!("ROUTINE_SCHEMA = {quoted}")));
    assert!(events_query(schema).contains(&format!("EVENT_SCHEMA = {quoted}")));
}

#[test]
fn parses_all_inventory_row_types() {
    assert_eq!(
        parse_view_row(&["account_balances".to_string(), "select 1".to_string()])
            .expect("view row"),
        ViewRow {
            table_name: "account_balances".to_string(),
            view_definition: "select 1".to_string(),
        }
    );
    assert_eq!(
        parse_trigger_row(&[
            "accounts_ai".to_string(),
            "INSERT".to_string(),
            "AFTER".to_string(),
            "accounts".to_string(),
            "insert into audit_log values (...)".to_string(),
        ])
        .expect("trigger row"),
        accounts_insert_trigger()
    );
    assert_eq!(
        parse_routine_row(&[
            "recalculate_accounts".to_string(),
            "PROCEDURE".to_string(),
            String::new(),
        ])
        .expect("routine row"),
        RoutineRow {
            routine_name: "recalculate_accounts".to_string(),
            routine_type: "PROCEDURE".to_string(),
            routine_definition: None,
        }
    );
    assert_eq!(
        parse_event_row(&[
            "nightly_recalc".to_string(),
            "ENABLED".to_string(),
            "call recalculate_accounts()".to_string(),
        ])
        .expect("event row"),
        nightly_recalc_event()
    );
}

#[test]
fn decodes_null_fields_and_ignores_blank_tsv_lines() {
    let rows = parse_tsv("\naccounts\tBASE TABLE\tNULL\tNULL\n\n");
    let table = parse_table_row(&rows[0]).expect("table row");

    assert_eq!(rows.len(), 1);
    assert_eq!(table.engine, None);
    assert_eq!(table.table_collation, None);
}

#[test]
fn reports_malformed_inventory_rows_with_row_type() {
    let error = parse_column_row(&["accounts".to_string()]).expect_err("short column row");

    assert_eq!(error.to_string(), "column row has 1 fields, expected 9");
}

#[test]
fn reports_invalid_numeric_inventory_fields_with_context() {
    let error = parse_primary_key_row(&[
        "accounts".to_string(),
        "id".to_string(),
        "not-a-number".to_string(),
    ])
    .expect_err("invalid ordinal");

    assert_eq!(
        error.to_string(),
        "primary key ordinal is not numeric: not-a-number"
    );
}

#[test]
fn orders_composite_primary_keys_by_ordinal_position() {
    let keys = group_primary_keys(vec![
        PrimaryKeyRow {
            table_name: "edges".to_string(),
            column_name: "right_id".to_string(),
            ordinal_position: 2,
        },
        PrimaryKeyRow {
            table_name: "edges".to_string(),
            column_name: "left_id".to_string(),
            ordinal_position: 1,
        },
    ]);

    assert_eq!(keys["edges"], vec!["left_id", "right_id"]);
}

#[test]
fn classifies_stored_generated_columns() {
    let column = build_column(ColumnRow {
        table_name: "accounts".to_string(),
        column_name: "balance_copy".to_string(),
        ordinal_position: 1,
        column_type: "int".to_string(),
        data_type: "int".to_string(),
        is_nullable: false,
        column_default: None,
        extra: "STORED GENERATED".to_string(),
        generation_expression: Some("`balance`".to_string()),
    });

    assert_eq!(
        column.generated,
        Some(GeneratedColumn {
            expression: "`balance`".to_string(),
            generation_kind: "STORED".to_string(),
        })
    );
}

#[test]
fn builds_empty_metadata_for_tables_without_related_rows() {
    let reader = FakeInventoryReader {
        tables: vec![TableRow {
            table_name: "no_columns_yet".to_string(),
            table_type: "BASE TABLE".to_string(),
            engine: Some("InnoDB".to_string()),
            table_collation: Some("utf8mb4_unicode_ci".to_string()),
        }],
        columns: Vec::new(),
        primary_keys: Vec::new(),
        views: Vec::new(),
        triggers: Vec::new(),
        routines: Vec::new(),
        events: Vec::new(),
    };

    let inventory = build_inventory("fixture_cdc", &reader).expect("inventory");

    assert_eq!(inventory.tables[0].name, "no_columns_yet");
    assert!(inventory.tables[0].columns.is_empty());
    assert!(inventory.tables[0].primary_key.is_empty());
}

struct FakeInventoryReader {
    tables: Vec<TableRow>,
    columns: Vec<ColumnRow>,
    primary_keys: Vec<PrimaryKeyRow>,
    views: Vec<ViewRow>,
    triggers: Vec<TriggerRow>,
    routines: Vec<RoutineRow>,
    events: Vec<EventRow>,
}

fn fake_reader() -> FakeInventoryReader {
    FakeInventoryReader {
        tables: vec![accounts_table()],
        columns: account_columns(),
        primary_keys: vec![account_primary_key()],
        views: vec![account_balances_view()],
        triggers: vec![accounts_insert_trigger()],
        routines: vec![recalculate_accounts_routine()],
        events: vec![nightly_recalc_event()],
    }
}

fn accounts_table() -> TableRow {
    TableRow {
        table_name: "accounts".to_string(),
        table_type: "BASE TABLE".to_string(),
        engine: Some("InnoDB".to_string()),
        table_collation: Some("utf8mb4_unicode_ci".to_string()),
    }
}

fn account_columns() -> Vec<ColumnRow> {
    vec![id_column(), name_column(), balance_x2_column()]
}

fn id_column() -> ColumnRow {
    ColumnRow {
        table_name: "accounts".to_string(),
        column_name: "id".to_string(),
        ordinal_position: 1,
        column_type: "int(11)".to_string(),
        data_type: "int".to_string(),
        is_nullable: false,
        column_default: None,
        extra: "auto_increment".to_string(),
        generation_expression: None,
    }
}

fn name_column() -> ColumnRow {
    ColumnRow {
        table_name: "accounts".to_string(),
        column_name: "name".to_string(),
        ordinal_position: 2,
        column_type: "varchar(64)".to_string(),
        data_type: "varchar".to_string(),
        is_nullable: false,
        column_default: None,
        extra: String::new(),
        generation_expression: None,
    }
}

fn balance_x2_column() -> ColumnRow {
    ColumnRow {
        table_name: "accounts".to_string(),
        column_name: "balance_x2".to_string(),
        ordinal_position: 3,
        column_type: "int(11)".to_string(),
        data_type: "int".to_string(),
        is_nullable: true,
        column_default: None,
        extra: "VIRTUAL GENERATED".to_string(),
        generation_expression: Some("`balance` * 2".to_string()),
    }
}

fn generated_balance_column() -> GeneratedColumn {
    GeneratedColumn {
        expression: "`balance` * 2".to_string(),
        generation_kind: "VIRTUAL".to_string(),
    }
}

fn account_primary_key() -> PrimaryKeyRow {
    PrimaryKeyRow {
        table_name: "accounts".to_string(),
        column_name: "id".to_string(),
        ordinal_position: 1,
    }
}

fn account_balances_view() -> ViewRow {
    ViewRow {
        table_name: "account_balances".to_string(),
        view_definition: "select id, balance from accounts".to_string(),
    }
}

fn accounts_insert_trigger() -> TriggerRow {
    TriggerRow {
        trigger_name: "accounts_ai".to_string(),
        event_manipulation: "INSERT".to_string(),
        action_timing: "AFTER".to_string(),
        event_object_table: "accounts".to_string(),
        action_statement: "insert into audit_log values (...)".to_string(),
    }
}

fn recalculate_accounts_routine() -> RoutineRow {
    RoutineRow {
        routine_name: "recalculate_accounts".to_string(),
        routine_type: "PROCEDURE".to_string(),
        routine_definition: Some("begin select 1; end".to_string()),
    }
}

fn nightly_recalc_event() -> EventRow {
    EventRow {
        event_name: "nightly_recalc".to_string(),
        status: "ENABLED".to_string(),
        event_definition: "call recalculate_accounts()".to_string(),
    }
}

impl InventoryReader for FakeInventoryReader {
    fn read_tables(&self, _schema: &str) -> Result<Vec<TableRow>, InventoryError> {
        Ok(self.tables.clone())
    }

    fn read_columns(&self, _schema: &str) -> Result<Vec<ColumnRow>, InventoryError> {
        Ok(self.columns.clone())
    }

    fn read_primary_keys(&self, _schema: &str) -> Result<Vec<PrimaryKeyRow>, InventoryError> {
        Ok(self.primary_keys.clone())
    }

    fn read_views(&self, _schema: &str) -> Result<Vec<ViewRow>, InventoryError> {
        Ok(self.views.clone())
    }

    fn read_triggers(&self, _schema: &str) -> Result<Vec<TriggerRow>, InventoryError> {
        Ok(self.triggers.clone())
    }

    fn read_routines(&self, _schema: &str) -> Result<Vec<RoutineRow>, InventoryError> {
        Ok(self.routines.clone())
    }

    fn read_events(&self, _schema: &str) -> Result<Vec<EventRow>, InventoryError> {
        Ok(self.events.clone())
    }
}
