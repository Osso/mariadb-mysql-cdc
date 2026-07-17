use crate::inventory::*;

pub(crate) struct FakeInventoryReader {
    pub(crate) tables: Vec<TableRow>,
    pub(crate) columns: Vec<ColumnRow>,
    pub(crate) primary_keys: Vec<PrimaryKeyRow>,
    pub(crate) indexes: Vec<IndexRow>,
    pub(crate) views: Vec<ViewRow>,
    pub(crate) triggers: Vec<TriggerRow>,
    pub(crate) routines: Vec<RoutineRow>,
    pub(crate) events: Vec<EventRow>,
}

pub(crate) fn fake_reader() -> FakeInventoryReader {
    FakeInventoryReader {
        tables: vec![accounts_table()],
        columns: account_columns(),
        primary_keys: vec![account_primary_key()],
        indexes: vec![account_name_index()],
        views: vec![account_balances_view()],
        triggers: vec![accounts_insert_trigger()],
        routines: vec![recalculate_accounts_routine()],
        events: vec![nightly_recalc_event()],
    }
}

pub(crate) fn accounts_table() -> TableRow {
    TableRow {
        table_name: "accounts".to_string(),
        table_type: "BASE TABLE".to_string(),
        engine: Some("InnoDB".to_string()),
        table_collation: Some("utf8mb4_unicode_ci".to_string()),
    }
}

pub(crate) fn account_balances_view_table() -> TableRow {
    TableRow {
        table_name: "account_balances".to_string(),
        table_type: "VIEW".to_string(),
        engine: None,
        table_collation: Some("utf8mb4_unicode_ci".to_string()),
    }
}

pub(crate) fn account_columns() -> Vec<ColumnRow> {
    vec![id_column(), name_column(), balance_x2_column()]
}

pub(crate) fn id_column() -> ColumnRow {
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

pub(crate) fn name_column() -> ColumnRow {
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

pub(crate) fn balance_x2_column() -> ColumnRow {
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

pub(crate) fn generated_balance_column() -> GeneratedColumn {
    GeneratedColumn {
        expression: "`balance` * 2".to_string(),
        generation_kind: "VIRTUAL".to_string(),
    }
}

pub(crate) fn account_name_index() -> IndexRow {
    IndexRow {
        table_name: "accounts".to_string(),
        index_name: "idx_accounts_name".to_string(),
        non_unique: false,
        index_type: "BTREE".to_string(),
        sequence: 1,
        column_name: Some("name".to_string()),
        prefix_length: None,
        collation: Some("A".to_string()),
        visible: true,
        comment: None,
    }
}

pub(crate) fn account_primary_key() -> PrimaryKeyRow {
    PrimaryKeyRow {
        table_name: "accounts".to_string(),
        column_name: "id".to_string(),
        ordinal_position: 1,
    }
}

pub(crate) fn account_balances_view() -> ViewRow {
    ViewRow {
        table_name: "account_balances".to_string(),
        view_definition: "select id, balance from accounts".to_string(),
    }
}

pub(crate) fn accounts_insert_trigger() -> TriggerRow {
    TriggerRow {
        trigger_name: "accounts_ai".to_string(),
        event_manipulation: "INSERT".to_string(),
        action_timing: "AFTER".to_string(),
        event_object_table: "accounts".to_string(),
        action_statement: "insert into audit_log values (...)".to_string(),
    }
}

pub(crate) fn recalculate_accounts_routine() -> RoutineRow {
    RoutineRow {
        routine_name: "recalculate_accounts".to_string(),
        routine_type: "PROCEDURE".to_string(),
        routine_definition: Some("begin select 1; end".to_string()),
    }
}

pub(crate) fn nightly_recalc_event() -> EventRow {
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

    fn read_indexes(&self, _schema: &str) -> Result<Vec<IndexRow>, InventoryError> {
        Ok(self.indexes.clone())
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
