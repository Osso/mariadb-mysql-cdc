use super::{GuestRangeBackend, GuestRangeRepairConfig};
use crate::canonical_foreign_key::CanonicalForeignKey;
use crate::database_row::DatabaseRow;
use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, MariaDbInventoryReader, TableInventory,
    build_canonical_foreign_key_inventory, build_inventory,
};
use crate::mysql_client::{sync_source_opts, sync_target_opts};
use crate::sync::config::sync_table_from_inventory;
use crate::sync::model::{SyncChunkReadRequest, SyncTable};
use crate::sync::mysql::{
    decode_optional_exact_row, decode_sync_rows, mysql_rows_to_strings, open_sync_connection,
    query_statement_rows_as_strings,
};
use crate::sync::sql::{
    build_exact_primary_key_select_statement, build_strict_insert_statement, build_sync_select_sql,
};
use mysql::prelude::Queryable;
use mysql::{Conn, Params};

pub(super) struct MySqlGuestRangeBackend {
    source: Conn,
    target: Conn,
    guests: SyncTable,
}

impl MySqlGuestRangeBackend {
    pub(super) fn connect(config: &GuestRangeRepairConfig) -> Result<Self, String> {
        let mut source = open_sync_connection(sync_source_opts(&config.source)?)
            .map_err(|e| format!("connect guest repair source: {e}"))?;
        let mut target = open_sync_connection(sync_target_opts(&config.target)?)
            .map_err(|e| format!("connect guest repair target: {e}"))?;
        control(&mut target, crate::live::target_session_init_command())?;
        control(
            &mut target,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )?;
        control(
            &mut source,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )?;
        control(
            &mut source,
            "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
        )?;
        let source_inventory = InventoryConfig {
            host: config.source.host.clone(),
            port: config.source.port,
            user: config.source.user.clone(),
            password: config.source.password.clone(),
            endpoint_role: InventoryEndpointRole::Source,
            use_tls: false,
            tls_ca_file: None,
            ..InventoryConfig::default()
        };
        let target_inventory = InventoryConfig {
            host: config.target.host.clone(),
            port: config.target.port,
            user: config.target.user.clone(),
            password: config.target.password.clone(),
            endpoint_role: InventoryEndpointRole::Target,
            use_tls: true,
            tls_ca_file: Some(config.target.tls_ca_file.clone()),
            ..InventoryConfig::default()
        };
        let guests = read_metadata(
            source_inventory,
            target_inventory,
            &config.source.database,
            &config.target.database,
        )?;
        Ok(Self {
            source,
            target,
            guests,
        })
    }
}

fn control(conn: &mut Conn, sql: &str) -> Result<(), String> {
    conn.query_drop(sql)
        .map_err(|e| format!("guest range session command {sql}: {e}"))
}

impl GuestRangeBackend for MySqlGuestRangeBackend {
    fn preflight(&mut self, config: &GuestRangeRepairConfig) -> Result<(u64, u64, u64), String> {
        let count: Option<(u64,Option<u64>,Option<u64>)>=self.source.exec_first(
            "SELECT COUNT(*),MIN(guest_id),MAX(guest_id) FROM guests WHERE guest_id BETWEEN ? AND ?",(config.start,config.end)
        ).map_err(|e|format!("count source guest range: {e}"))?;
        let Some((count, Some(start), Some(end))) = count else {
            return Err("source range is empty".into());
        };
        Ok((count, start, end))
    }
    fn read_page(
        &mut self,
        config: &GuestRangeRepairConfig,
        after: Option<u64>,
    ) -> Result<Vec<DatabaseRow>, String> {
        let request = SyncChunkReadRequest {
            start_after: after
                .or_else(|| config.start.checked_sub(1))
                .map(|id| vec![id.to_string()]),
            end_at: Some(vec![config.end.to_string()]),
            limit: config.batch_size,
        };
        let sql = build_sync_select_sql(&self.guests, &request);
        let rows = self
            .source
            .query::<mysql::Row, _>(sql)
            .map_err(|e| format!("read source guest range page: {e}"))?;
        decode_sync_rows(&self.guests, mysql_rows_to_strings(rows))
    }
    fn begin_batch(&mut self) -> Result<(), String> {
        control(&mut self.target, "START TRANSACTION")
    }
    fn require_parent(&mut self, row: &DatabaseRow) -> Result<(), String> {
        let utm = row
            .values
            .get("utm_id")
            .ok_or("guest row missing utm_id column")?;
        let Some(utm) = utm else {
            return Ok(());
        };
        let ids: Vec<mysql::Row> = self
            .target
            .exec(
                "SELECT id FROM utms WHERE id = ? LIMIT 2 LOCK IN SHARE MODE",
                (utm,),
            )
            .map_err(|e| format!("lock required target utms row: {e}"))?;
        if ids.len() != 1 {
            return Err(format!(
                "required target utms row {utm} is absent or ambiguous"
            ));
        }
        Ok(())
    }
    fn read_target(&mut self, row: &DatabaseRow) -> Result<Option<DatabaseRow>, String> {
        let mut statement =
            build_exact_primary_key_select_statement(&self.guests, &row.primary_key)?;
        statement.sql.push_str(" FOR UPDATE");
        let rows = query_statement_rows_as_strings(
            &mut self.target,
            &statement,
            "lock/read target guest",
        )?;
        decode_optional_exact_row(&self.guests, rows, "target guest")
    }
    fn insert(&mut self, row: &DatabaseRow) -> Result<(), String> {
        let statement = build_strict_insert_statement(&self.guests, std::slice::from_ref(row))?;
        self.target
            .exec_drop(statement.sql, Params::Positional(statement.params))
            .map_err(|e| format!("strict guest insert {:?}: {e}", row.primary_key))?;
        if self.target.affected_rows() != 1 {
            return Err("strict guest insert did not affect exactly one row".into());
        }
        Ok(())
    }
    fn commit(&mut self) -> Result<(), String> {
        control(&mut self.target, "COMMIT")
    }
    fn rollback(&mut self) -> Result<(), String> {
        control(&mut self.target, "ROLLBACK")
    }
    fn finish_source(&mut self) -> Result<(), String> {
        control(&mut self.source, "ROLLBACK")
    }
}

fn read_metadata(
    source: InventoryConfig,
    target: InventoryConfig,
    source_db: &str,
    target_db: &str,
) -> Result<SyncTable, String> {
    let source = MariaDbInventoryReader::new(source);
    let target = MariaDbInventoryReader::new(target);
    let (source_guests, source_fks) = read_table(&source, source_db, "guests")?;
    let (target_guests, target_fks) = read_table(&target, target_db, "guests")?;
    let guests = validate_guests_metadata(&source_guests, &target_guests)?;
    validate_guest_fk(source_db, &source_fks, false)?;
    validate_guest_fk(target_db, &target_fks, true)?;
    let (source_utms, _) = read_table(&source, source_db, "utms")?;
    let (target_utms, _) = read_table(&target, target_db, "utms")?;
    validate_table_pair(&source_utms, &target_utms)?;
    if source_utms.primary_key != ["id"] {
        return Err("utms primary key must be exactly id".into());
    }
    Ok(guests)
}

fn read_table(
    reader: &MariaDbInventoryReader,
    database: &str,
    name: &str,
) -> Result<(TableInventory, Vec<CanonicalForeignKey>), String> {
    reader.scope_to_table(name);
    let inventory = build_inventory(database, reader)
        .map_err(|e| format!("read guest repair {name} inventory: {e}"))?;
    let table = inventory
        .tables
        .into_iter()
        .find(|t| t.name == name)
        .ok_or_else(|| format!("missing guest repair table {name}"))?;
    let fks = build_canonical_foreign_key_inventory(database, reader)
        .map_err(|e| format!("read guest repair {name} foreign keys: {e}"))?;
    Ok((table, fks))
}

fn validate_table_pair(
    source: &TableInventory,
    target: &TableInventory,
) -> Result<SyncTable, String> {
    for table in [source, target] {
        if table.table_type != "BASE TABLE" || table.engine.as_deref() != Some("InnoDB") {
            return Err(format!("{} must be an InnoDB base table", table.name));
        }
        if table
            .columns
            .iter()
            .any(|column| column.generated.is_some())
        {
            return Err(format!(
                "{} generated columns cannot be copied as full source values",
                table.name
            ));
        }
    }
    let source_sync = sync_table_from_inventory(source)?;
    if source_sync != sync_table_from_inventory(target)? {
        return Err(format!(
            "{} source/target typed metadata differs",
            source.name
        ));
    }
    for (left, right) in source.columns.iter().zip(&target.columns) {
        validate_column_pair(left, right)?;
    }
    Ok(source_sync)
}

fn validate_column_pair(
    left: &crate::inventory::ColumnInventory,
    right: &crate::inventory::ColumnInventory,
) -> Result<(), String> {
    let left_type = (
        normalized_column_type(left),
        left.is_nullable,
        &left.character_set,
        &left.collation,
    );
    let right_type = (
        normalized_column_type(right),
        right.is_nullable,
        &right.character_set,
        &right.collation,
    );
    if left_type != right_type {
        return Err(format!(
            "column {} source/target type, nullability, charset or collation differs",
            left.name
        ));
    }
    Ok(())
}

fn normalized_column_type(column: &crate::inventory::ColumnInventory) -> String {
    if !matches!(
        column.data_type.as_str(),
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
    ) {
        return column.column_type.clone();
    }
    let suffix = column
        .column_type
        .strip_prefix(&column.data_type)
        .unwrap_or(&column.column_type);
    let suffix = if suffix.starts_with('(') {
        suffix.split_once(')').map_or(suffix, |(_, rest)| rest)
    } else {
        suffix
    };
    format!("{}{suffix}", column.data_type)
}

fn validate_guests_metadata(
    source: &TableInventory,
    target: &TableInventory,
) -> Result<SyncTable, String> {
    let mut table = validate_table_pair(source, target)?;
    if table.name != "guests" || table.primary_key != ["guest_id"] {
        return Err("guests primary key must be exactly guest_id".into());
    }
    if !table.columns.iter().any(|name| name == "utm_id") {
        return Err("guests requires utm_id".into());
    }
    let id = source
        .columns
        .iter()
        .find(|column| column.name == "guest_id")
        .ok_or("missing guest_id metadata")?;
    if !matches!(
        id.data_type.as_str(),
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
    ) {
        return Err("guest_id must have an integer type".into());
    }
    // This existing field selects HEX reads and raw byte parameters, despite its narrow name.
    table.mediumblob_columns = source
        .columns
        .iter()
        .filter(|column| {
            matches!(
                column.data_type.as_str(),
                "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
            )
        })
        .map(|column| column.name.clone())
        .collect();
    Ok(table)
}

fn validate_guest_fk(
    database: &str,
    fks: &[CanonicalForeignKey],
    target: bool,
) -> Result<(), String> {
    let [fk] = fks else {
        return Err("guests must have exactly one canonical foreign key to utms".into());
    };
    let name = if target {
        "guests_fk_guests_utm_id"
    } else {
        "fk_guests_utm_id"
    };
    let expected = CanonicalForeignKey {
        constraint_schema: database.into(),
        constraint_name: name.into(),
        child_schema: database.into(),
        child_table: "guests".into(),
        child_columns: vec!["utm_id".into()],
        parent_schema: database.into(),
        parent_table: "utms".into(),
        parent_columns: vec!["id".into()],
        update_rule: "RESTRICT".into(),
        delete_rule: "RESTRICT".into(),
        match_option: "NONE".into(),
        enforced: true,
    };
    if *fk != expected {
        return Err("guests canonical FK differs from required guests(utm_id) -> utms(id) RESTRICT contract".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::ColumnInventory;

    fn table() -> TableInventory {
        let column =
            |name: &str, data_type: &str, column_type: &str, ordinal_position| ColumnInventory {
                name: name.into(),
                ordinal_position,
                data_type: data_type.into(),
                column_type: column_type.into(),
                is_nullable: name == "utm_id",
                character_set: None,
                collation: None,
                default_value: None,
                extra: String::new(),
                comment: String::new(),
                generated: None,
            };
        TableInventory {
            name: "guests".into(),
            table_type: "BASE TABLE".into(),
            engine: Some("InnoDB".into()),
            collation: None,
            primary_key: vec!["guest_id".into()],
            columns: vec![
                column("guest_id", "bigint", "bigint(20) unsigned", 1),
                column("guest_hash", "varchar", "varchar(64)", 2),
                column("utm_id", "bigint", "bigint(20) unsigned", 3),
            ],
        }
    }

    #[test]
    fn binary_guest_values_bind_original_bytes_not_hex_text() {
        for kind in [
            "binary",
            "varbinary",
            "tinyblob",
            "blob",
            "mediumblob",
            "longblob",
        ] {
            let mut inventory = table();
            inventory.columns[1].data_type = kind.into();
            inventory.columns[1].column_type = kind.into();
            let metadata = validate_guests_metadata(&inventory, &inventory).unwrap();
            let rows = decode_sync_rows(
                &metadata,
                vec![vec![
                    Some("100".into()),
                    Some("00FF8064".into()),
                    Some("7".into()),
                ]],
            )
            .unwrap();
            let statement = build_strict_insert_statement(&metadata, &rows).unwrap();
            assert_eq!(
                statement.params[1],
                mysql::Value::Bytes(vec![0x00, 0xff, 0x80, 0x64]),
                "{kind}"
            );
            let rows =
                decode_sync_rows(&metadata, vec![vec![Some("101".into()), None, None]]).unwrap();
            assert_eq!(
                build_strict_insert_statement(&metadata, &rows)
                    .unwrap()
                    .params[1],
                mysql::Value::NULL
            );
        }
    }

    #[test]
    fn metadata_rejects_changed_value_width_and_collation() {
        let source = table();
        let mut target = source.clone();
        target.columns[1].column_type = "varchar(32)".into();
        assert!(validate_guests_metadata(&source, &target).is_err());
        let mut target = source.clone();
        target.columns[1].collation = Some("utf8mb4_bin".into());
        assert!(validate_guests_metadata(&source, &target).is_err());
    }

    #[test]
    fn metadata_allows_only_integer_display_width_difference() {
        let source = table();
        let mut target = source.clone();
        target.columns[0].column_type = "bigint unsigned".into();
        assert!(validate_guests_metadata(&source, &target).is_ok());
        target.columns[0].column_type = "bigint".into();
        assert!(validate_guests_metadata(&source, &target).is_err());
    }

    #[test]
    fn metadata_rejects_wrong_pk_engine_and_nullability() {
        let source = table();
        let mut target = source.clone();
        target.primary_key = vec!["utm_id".into()];
        assert!(validate_guests_metadata(&source, &target).is_err());
        let mut target = source.clone();
        target.engine = Some("MyISAM".into());
        assert!(validate_guests_metadata(&source, &target).is_err());
        let mut target = source.clone();
        target.columns[2].is_nullable = false;
        assert!(validate_guests_metadata(&source, &target).is_err());
    }

    #[test]
    fn canonical_fk_checks_mapped_name_rules_and_local_parent() {
        let fk = CanonicalForeignKey {
            constraint_schema: "fixture".into(),
            constraint_name: "fk_guests_utm_id".into(),
            child_schema: "fixture".into(),
            child_table: "guests".into(),
            child_columns: vec!["utm_id".into()],
            parent_schema: "fixture".into(),
            parent_table: "utms".into(),
            parent_columns: vec!["id".into()],
            update_rule: "RESTRICT".into(),
            delete_rule: "RESTRICT".into(),
            match_option: "NONE".into(),
            enforced: true,
        };
        assert!(validate_guest_fk("fixture", std::slice::from_ref(&fk), false).is_ok());
        assert!(validate_guest_fk("fixture", std::slice::from_ref(&fk), true).is_err());
        let mut mapped = fk;
        mapped.constraint_name = "guests_fk_guests_utm_id".into();
        assert!(validate_guest_fk("fixture", std::slice::from_ref(&mapped), true).is_ok());
        let mut wrong = mapped.clone();
        wrong.parent_schema = "another".into();
        assert!(validate_guest_fk("fixture", &[wrong], true).is_err());
        let mut wrong = mapped.clone();
        wrong.delete_rule = "CASCADE".into();
        assert!(validate_guest_fk("fixture", &[wrong], true).is_err());
        let mut wrong = mapped;
        wrong.enforced = false;
        assert!(validate_guest_fk("fixture", &[wrong], true).is_err());
        assert!(validate_guest_fk("fixture", &[], true).is_err());
    }
}
