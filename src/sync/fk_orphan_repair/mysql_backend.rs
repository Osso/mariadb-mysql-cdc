use super::{
    ArtistMetadata, FkOrphanRepairBackend, FkOrphanRepairCase, FkOrphanRepairConfig,
    FkOrphanRepairReport, RepairCaseSpec, RepairMetadata, required_row_value,
};
use crate::canonical_foreign_key::CanonicalForeignKey;
use crate::database_row::DatabaseRow;
use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, MariaDbInventoryReader,
    build_canonical_foreign_key_inventory, build_inventory,
};
use crate::mysql_client::{sync_source_opts, sync_target_opts};
use crate::mysql_support::quote_ident;
use crate::sync::config::sync_table_from_inventory;
use crate::sync::model::SyncTable;
use crate::sync::mysql::{
    decode_optional_exact_row, mysql_rows_to_strings, open_sync_connection,
    query_statement_rows_as_strings,
};
use crate::sync::sql::{
    build_exact_primary_key_select_statement, build_strict_delete_rows_statement,
    build_strict_insert_statement, build_strict_update_rows_statement,
};
use crate::target::SqlStatement;
use mysql::prelude::Queryable;
use mysql::{Conn, Params, Value};

pub(super) fn run_mysql_fk_orphan_repair(
    config: &FkOrphanRepairConfig,
) -> Result<FkOrphanRepairReport, String> {
    let mut backend = MySqlFkOrphanRepairBackend::connect(config)?;
    super::repair_with_backend(config, &mut backend)
}

struct MySqlFkOrphanRepairBackend {
    source_config: crate::mysql_config::MySqlConnectionConfig,
    target_config: crate::live::TargetMySqlConfig,
    source: Conn,
    target: Conn,
}

impl MySqlFkOrphanRepairBackend {
    fn connect(config: &FkOrphanRepairConfig) -> Result<Self, String> {
        let source_opts = sync_source_opts(&config.source)?;
        let source = open_sync_connection(source_opts)
            .map_err(|error| format!("connect to source mysql for FK orphan repair: {error}"))?;
        let target_opts = sync_target_opts(&config.target)?;
        let mut target = open_sync_connection(target_opts)
            .map_err(|error| format!("connect to target mysql for FK orphan repair: {error}"))?;
        target
            .query_drop(crate::live::target_session_init_command())
            .map_err(|error| format!("initialize target mysql repair session: {error}"))?;
        Ok(Self {
            source_config: config.source.clone(),
            target_config: config.target.clone(),
            source,
            target,
        })
    }

    fn execute_target_control(&mut self, sql: &str, operation: &str) -> Result<(), String> {
        self.target
            .query_drop(sql)
            .map_err(|error| format!("{operation}: {error}"))
    }

    fn query_exact_source_row(
        &mut self,
        table: &SyncTable,
        primary_key: &[String],
        endpoint: &str,
    ) -> Result<Option<DatabaseRow>, String> {
        let statement = build_exact_primary_key_select_statement(table, primary_key)?;
        let rows = query_statement_rows_as_strings(&mut self.source, &statement, endpoint)?;
        decode_optional_exact_row(table, rows, endpoint)
    }

    fn query_exact_target_row(
        &mut self,
        table: &SyncTable,
        primary_key: &[String],
        endpoint: &str,
    ) -> Result<Option<DatabaseRow>, String> {
        let statement = build_exact_primary_key_select_statement(table, primary_key)?;
        let rows = query_statement_rows_as_strings(&mut self.target, &statement, endpoint)?;
        decode_optional_exact_row(table, rows, endpoint)
    }
}

impl FkOrphanRepairBackend for MySqlFkOrphanRepairBackend {
    fn validate_case(&mut self, spec: &RepairCaseSpec) -> Result<RepairMetadata, String> {
        read_repair_metadata(&self.source_config, &self.target_config, spec)
    }

    fn orphan_keys(
        &mut self,
        spec: &RepairCaseSpec,
        metadata: &RepairMetadata,
        limit: usize,
    ) -> Result<Vec<Vec<String>>, String> {
        let query_limit = limit
            .checked_add(1)
            .ok_or_else(|| "FK orphan query limit overflow".to_string())?;
        let sql = build_orphan_keys_sql(spec, &metadata.target_child, query_limit);
        let rows = self
            .target
            .query::<mysql::Row, _>(&sql)
            .map_err(|error| format!("query target FK orphan identities: {error}"))?;
        let primary_keys =
            decode_primary_keys(&metadata.target_child, mysql_rows_to_strings(rows))?;
        if primary_keys.len() > limit {
            return Err(format!(
                "target FK orphan count for `{}` exceeds limit {limit}",
                spec.name
            ));
        }
        Ok(primary_keys)
    }

    fn begin_batch(
        &mut self,
        spec: &RepairCaseSpec,
        _metadata: &RepairMetadata,
    ) -> Result<(), String> {
        self.execute_target_control("SET autocommit=0", "disable target repair autocommit")?;
        let sql = build_lock_tables_sql(&self.target_config.database, spec);
        self.execute_target_control(&sql, "lock target repair tables")
    }

    fn is_target_orphan(
        &mut self,
        spec: &RepairCaseSpec,
        metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<bool, String> {
        let statement = build_exact_orphan_statement(spec, &metadata.target_child, primary_key)?;
        let rows = query_statement_rows_as_strings(
            &mut self.target,
            &statement,
            "query exact target FK orphan",
        )?;
        match rows.len() {
            0 => Ok(false),
            1 => Ok(true),
            count => Err(format!(
                "exact target FK orphan query returned {count} rows for primary key {primary_key:?}"
            )),
        }
    }

    fn read_source_child(
        &mut self,
        metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<Option<DatabaseRow>, String> {
        self.query_exact_source_row(&metadata.source_child, primary_key, "source repair child")
    }

    fn read_source_parent(
        &mut self,
        spec: &RepairCaseSpec,
        metadata: &RepairMetadata,
        child: &DatabaseRow,
    ) -> Result<Option<DatabaseRow>, String> {
        let primary_key = parent_primary_key(spec, child, "source child")?;
        self.query_exact_source_row(
            &metadata.source_parent,
            &primary_key,
            "source repair parent",
        )
    }

    fn read_target_child(
        &mut self,
        metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<Option<DatabaseRow>, String> {
        self.query_exact_target_row(&metadata.target_child, primary_key, "target repair child")
    }

    fn read_target_parent(
        &mut self,
        spec: &RepairCaseSpec,
        metadata: &RepairMetadata,
        child: &DatabaseRow,
    ) -> Result<Option<DatabaseRow>, String> {
        let primary_key = parent_primary_key(spec, child, "target child")?;
        self.query_exact_target_row(
            &metadata.target_parent,
            &primary_key,
            "target repair parent",
        )
    }

    fn read_source_artist(
        &mut self,
        metadata: &ArtistMetadata,
        primary_key: &[String],
    ) -> Result<Option<DatabaseRow>, String> {
        self.query_exact_source_row(&metadata.source, primary_key, "source repair artist")
    }

    fn read_target_artist(
        &mut self,
        metadata: &ArtistMetadata,
        primary_key: &[String],
    ) -> Result<Option<DatabaseRow>, String> {
        self.query_exact_target_row(&metadata.target, primary_key, "target repair artist")
    }

    fn insert_target_artist(
        &mut self,
        metadata: &ArtistMetadata,
        artist: &DatabaseRow,
    ) -> Result<(), String> {
        let statement =
            build_strict_insert_statement(&metadata.target, std::slice::from_ref(artist))?;
        execute_exact_target_mutation(&mut self.target, statement, "insert missing target artist")
    }

    fn restore_target_parent(
        &mut self,
        metadata: &RepairMetadata,
        parent: &DatabaseRow,
        exists: bool,
    ) -> Result<(), String> {
        let rows = std::slice::from_ref(parent);
        let statement = if exists {
            build_strict_update_rows_statement(&metadata.target_parent, rows)?
        } else {
            build_strict_insert_statement(&metadata.target_parent, rows)?
        };
        execute_exact_target_mutation(&mut self.target, statement, "restore target repair parent")
    }

    fn update_target_child(
        &mut self,
        metadata: &RepairMetadata,
        child: &DatabaseRow,
    ) -> Result<(), String> {
        let statement = build_strict_update_rows_statement(
            &metadata.target_child,
            std::slice::from_ref(child),
        )?;
        execute_exact_target_mutation(&mut self.target, statement, "update target repair child")
    }

    fn delete_target_child(
        &mut self,
        metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<(), String> {
        let statement =
            build_strict_delete_rows_statement(&metadata.target_child, &[primary_key.to_vec()])?;
        execute_exact_target_mutation(&mut self.target, statement, "delete target repair child")
    }

    fn commit_batch(&mut self) -> Result<(), String> {
        self.execute_target_control("COMMIT", "commit target repair batch")
    }

    fn rollback_batch(&mut self) -> Result<(), String> {
        self.execute_target_control("ROLLBACK", "rollback target repair batch")
    }

    fn unlock_batch(&mut self) -> Result<(), String> {
        self.execute_target_control("UNLOCK TABLES", "unlock target repair tables")
    }
}

fn read_repair_metadata(
    source: &crate::mysql_config::MySqlConnectionConfig,
    target: &crate::live::TargetMySqlConfig,
    spec: &RepairCaseSpec,
) -> Result<RepairMetadata, String> {
    let source_reader = MariaDbInventoryReader::new(source_inventory_config(source));
    let (source_child, source_foreign_keys) =
        read_scoped_table(&source_reader, &source.database, spec.child_table)?;
    let (source_parent, source_parent_foreign_keys) =
        read_scoped_table(&source_reader, &source.database, spec.parent_table)?;
    validate_source_foreign_key(&source.database, spec, &source_foreign_keys)?;

    let target_reader = MariaDbInventoryReader::new(target_inventory_config(target));
    let (target_child, target_foreign_keys) =
        read_scoped_table(&target_reader, &target.database, spec.child_table)?;
    let (target_parent, _) =
        read_scoped_table(&target_reader, &target.database, spec.parent_table)?;
    validate_target_foreign_key_absent(spec, &target_foreign_keys)?;
    validate_table_pair(spec.child_table, &source_child, &target_child)?;
    validate_table_pair(spec.parent_table, &source_parent, &target_parent)?;
    validate_case_columns(spec, &source_child, &source_parent)?;

    let artists = if spec.restores_comics_parent() {
        Some(read_artist_metadata(
            source,
            target,
            &source_parent,
            &source_parent_foreign_keys,
        )?)
    } else {
        None
    };
    Ok(RepairMetadata {
        artists,
        source_child,
        target_child,
        source_parent,
        target_parent,
    })
}

fn read_artist_metadata(
    source: &crate::mysql_config::MySqlConnectionConfig,
    target: &crate::live::TargetMySqlConfig,
    comics: &SyncTable,
    comics_foreign_keys: &[CanonicalForeignKey],
) -> Result<ArtistMetadata, String> {
    let spec = FkOrphanRepairCase::Comics.spec();
    validate_source_foreign_key(&source.database, &spec, comics_foreign_keys)?;
    let source_reader = MariaDbInventoryReader::new(source_inventory_config(source));
    let (source_artist, foreign_keys) =
        read_scoped_table(&source_reader, &source.database, "artists")?;
    if !foreign_keys.is_empty() {
        return Err(
            "source artists has unexpected foreign keys; ancestor repair is not recursive".into(),
        );
    }
    let target_reader = MariaDbInventoryReader::new(target_inventory_config(target));
    let (target_artist, _) = read_scoped_table(&target_reader, &target.database, "artists")?;
    validate_table_pair("artists", &source_artist, &target_artist)?;
    validate_case_columns(&spec, comics, &source_artist)?;
    Ok(ArtistMetadata {
        source: source_artist,
        target: target_artist,
    })
}

fn source_inventory_config(source: &crate::mysql_config::MySqlConnectionConfig) -> InventoryConfig {
    InventoryConfig {
        host: source.host.clone(),
        port: source.port,
        user: source.user.clone(),
        password: source.password.clone(),
        endpoint_role: InventoryEndpointRole::Source,
        use_tls: false,
        tls_ca_file: None,
        ..InventoryConfig::default()
    }
}

fn target_inventory_config(target: &crate::live::TargetMySqlConfig) -> InventoryConfig {
    InventoryConfig {
        host: target.host.clone(),
        port: target.port,
        user: target.user.clone(),
        password: target.password.clone(),
        endpoint_role: InventoryEndpointRole::Target,
        use_tls: true,
        tls_ca_file: Some(target.tls_ca_file.clone()),
        ..InventoryConfig::default()
    }
}

fn read_scoped_table(
    reader: &MariaDbInventoryReader,
    database: &str,
    table_name: &str,
) -> Result<(SyncTable, Vec<CanonicalForeignKey>), String> {
    reader.scope_to_table(table_name);
    let inventory = build_inventory(database, reader)
        .map_err(|error| format!("read `{table_name}` repair inventory: {error}"))?;
    let table = inventory
        .tables
        .iter()
        .find(|table| table.name == table_name)
        .ok_or_else(|| format!("repair table `{table_name}` is missing from `{database}`"))?;
    let sync_table = sync_table_from_inventory(table)?;
    let foreign_keys = build_canonical_foreign_key_inventory(database, reader)
        .map_err(|error| format!("read `{table_name}` repair foreign keys: {error}"))?;
    Ok((sync_table, foreign_keys))
}

fn validate_source_foreign_key(
    database: &str,
    spec: &RepairCaseSpec,
    foreign_keys: &[CanonicalForeignKey],
) -> Result<(), String> {
    let matches = foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.constraint_name == spec.constraint_name)
        .collect::<Vec<_>>();
    let [foreign_key] = matches.as_slice() else {
        return Err(format!(
            "source constraint `{}` must exist exactly once; found {}",
            spec.constraint_name,
            matches.len()
        ));
    };
    let expected = CanonicalForeignKey {
        constraint_schema: database.to_string(),
        constraint_name: spec.constraint_name.to_string(),
        child_schema: database.to_string(),
        child_table: spec.child_table.to_string(),
        child_columns: strings(spec.child_foreign_key),
        parent_schema: database.to_string(),
        parent_table: spec.parent_table.to_string(),
        parent_columns: strings(spec.parent_key),
        update_rule: spec.update_rule.to_string(),
        delete_rule: spec.delete_rule.to_string(),
        match_option: "NONE".to_string(),
        enforced: true,
    };
    if **foreign_key != expected {
        return Err(format!(
            "source constraint `{}` differs from the allowlisted repair contract",
            spec.constraint_name
        ));
    }
    Ok(())
}

fn validate_target_foreign_key_absent(
    spec: &RepairCaseSpec,
    foreign_keys: &[CanonicalForeignKey],
) -> Result<(), String> {
    let conflicting = foreign_keys.iter().find(|foreign_key| {
        foreign_key.constraint_name == spec.constraint_name
            || (foreign_key.child_table == spec.child_table
                && foreign_key.child_columns == strings(spec.child_foreign_key)
                && foreign_key.parent_table == spec.parent_table
                && foreign_key.parent_columns == strings(spec.parent_key))
    });
    if let Some(foreign_key) = conflicting {
        return Err(format!(
            "target FK `{}` already covers allowlisted repair case `{}`",
            foreign_key.constraint_name, spec.name
        ));
    }
    Ok(())
}

fn validate_table_pair(
    table_name: &str,
    source: &SyncTable,
    target: &SyncTable,
) -> Result<(), String> {
    if source != target {
        return Err(format!(
            "source and target writable metadata differ for repair table `{table_name}`"
        ));
    }
    Ok(())
}

fn validate_case_columns(
    spec: &RepairCaseSpec,
    child: &SyncTable,
    parent: &SyncTable,
) -> Result<(), String> {
    if child.primary_key != strings(spec.child_primary_key) {
        return Err(format!(
            "child primary key for `{}` differs from allowlisted repair contract",
            spec.child_table
        ));
    }
    if parent.primary_key != strings(spec.parent_primary_key) {
        return Err(format!(
            "parent primary key for `{}` differs from allowlisted repair contract",
            spec.parent_table
        ));
    }
    if spec.restores_comics_parent() {
        require_columns(child, &["comic_id"], "explicit comics parent identity")?;
    }
    require_columns(child, spec.child_foreign_key, "child foreign key")?;
    require_columns(parent, spec.parent_key, "parent key")
}

fn require_columns(table: &SyncTable, columns: &[&str], label: &str) -> Result<(), String> {
    for column in columns {
        if !table.columns.iter().any(|candidate| candidate == column) {
            return Err(format!(
                "{label} column `{column}` is absent from `{}`",
                table.name
            ));
        }
    }
    Ok(())
}

fn build_orphan_keys_sql(spec: &RepairCaseSpec, child: &SyncTable, limit: usize) -> String {
    let selected_primary_key = qualified_columns(spec.child_table, &child.primary_key);
    let join = foreign_key_join(spec);
    let non_null = non_null_foreign_key_filter(spec);
    let missing_parent = format!(
        "{} IS NULL",
        qualified(spec.parent_table, spec.parent_primary_key[0])
    );
    let order_by = qualified_columns(spec.child_table, &child.primary_key);
    format!(
        "SELECT {selected_primary_key} FROM {} LEFT JOIN {} ON {join} WHERE {non_null} AND {missing_parent} ORDER BY {order_by} LIMIT {limit}",
        quote_ident(spec.child_table),
        quote_ident(spec.parent_table),
    )
}

fn build_exact_orphan_statement(
    spec: &RepairCaseSpec,
    child: &SyncTable,
    primary_key: &[String],
) -> Result<SqlStatement, String> {
    if primary_key.len() != child.primary_key.len() {
        return Err(format!(
            "exact repair primary-key width mismatch for `{}`: expected {}, found {}",
            child.name,
            child.primary_key.len(),
            primary_key.len()
        ));
    }
    let primary_key_filter = exact_primary_key_filter(spec.child_table, &child.primary_key);
    let non_null = non_null_foreign_key_filter(spec);
    let missing_parent = format!(
        "{} IS NULL",
        qualified(spec.parent_table, spec.parent_primary_key[0])
    );
    Ok(SqlStatement {
        sql: format!(
            "SELECT 1 FROM {} LEFT JOIN {} ON {} WHERE {primary_key_filter} AND {non_null} AND {missing_parent} LIMIT 2",
            quote_ident(spec.child_table),
            quote_ident(spec.parent_table),
            foreign_key_join(spec),
        ),
        params: primary_key
            .iter()
            .map(|value| Value::Bytes(value.as_bytes().to_vec()))
            .collect(),
    })
}

fn exact_primary_key_filter(table: &str, primary_key: &[String]) -> String {
    primary_key
        .iter()
        .map(|column| format!("{} = ?", qualified(table, column)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn non_null_foreign_key_filter(spec: &RepairCaseSpec) -> String {
    spec.child_foreign_key
        .iter()
        .map(|column| format!("{} IS NOT NULL", qualified(spec.child_table, column)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn build_lock_tables_sql(database: &str, spec: &RepairCaseSpec) -> String {
    if spec.restores_comics_parent() {
        return format!(
            "LOCK TABLES {}.`artists` WRITE, {}.`comics` WRITE, {}.{} WRITE",
            quote_ident(database),
            quote_ident(database),
            quote_ident(database),
            quote_ident(spec.child_table),
        );
    }
    format!(
        "LOCK TABLES {}.{} WRITE, {}.{} {}",
        quote_ident(database),
        quote_ident(spec.child_table),
        quote_ident(database),
        quote_ident(spec.parent_table),
        "READ"
    )
}

fn foreign_key_join(spec: &RepairCaseSpec) -> String {
    spec.child_foreign_key
        .iter()
        .zip(spec.parent_key.iter())
        .map(|(child, parent)| {
            format!(
                "{} = {}",
                qualified(spec.child_table, child),
                qualified(spec.parent_table, parent)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn qualified_columns(table: &str, columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| qualified(table, column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn qualified(table: &str, column: &str) -> String {
    format!("{}.{}", quote_ident(table), quote_ident(column))
}

fn decode_primary_keys(
    table: &SyncTable,
    rows: Vec<Vec<Option<String>>>,
) -> Result<Vec<Vec<String>>, String> {
    rows.into_iter()
        .map(|row| {
            if row.len() != table.primary_key.len() {
                return Err(format!(
                    "target FK orphan identity width mismatch for `{}`: expected {}, found {}",
                    table.name,
                    table.primary_key.len(),
                    row.len()
                ));
            }
            row.into_iter()
                .zip(table.primary_key.iter())
                .map(|(value, column)| {
                    value.ok_or_else(|| {
                        format!(
                            "target FK orphan primary-key column `{column}` is NULL for `{}`",
                            table.name
                        )
                    })
                })
                .collect()
        })
        .collect()
}

fn parent_primary_key(
    spec: &RepairCaseSpec,
    child: &DatabaseRow,
    endpoint: &str,
) -> Result<Vec<String>, String> {
    if spec.restores_comics_parent() {
        return Ok(vec![
            required_row_value(child, "comic_id", endpoint)?.to_string(),
        ]);
    }
    spec.parent_primary_key
        .iter()
        .map(|parent_column| {
            let position = spec
                .parent_key
                .iter()
                .position(|candidate| candidate == parent_column)
                .ok_or_else(|| {
                    format!("parent PK column `{parent_column}` is not referenced by repair FK")
                })?;
            required_row_value(child, spec.child_foreign_key[position], endpoint)
                .map(ToString::to_string)
        })
        .collect()
}

fn execute_exact_target_mutation(
    target: &mut Conn,
    statement: SqlStatement,
    operation: &str,
) -> Result<(), String> {
    target
        .exec_drop(&statement.sql, Params::Positional(statement.params))
        .map_err(|error| format!("{operation}: {error}"))?;
    let affected = target.affected_rows();
    if affected != 1 {
        return Err(format!("{operation} affected {affected} rows; expected 1"));
    }
    Ok(())
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::model::SyncPrimaryKeyOrdering;

    #[test]
    fn orphan_query_is_bounded_and_uses_exact_allowlisted_join() {
        let spec = super::super::FkOrphanRepairCase::PhrasesSuggestions.spec();
        let child = sync_table(spec.child_table, spec.child_primary_key);

        let sql = build_orphan_keys_sql(&spec, &child, 2);

        assert_eq!(
            sql,
            "SELECT `phrases_suggestions`.`id`, `phrases_suggestions`.`lang`, `phrases_suggestions`.`author_id` FROM `phrases_suggestions` LEFT JOIN `users` ON `phrases_suggestions`.`author_id` = `users`.`id` AND `phrases_suggestions`.`author_username` = `users`.`name` WHERE `phrases_suggestions`.`author_id` IS NOT NULL AND `phrases_suggestions`.`author_username` IS NOT NULL AND `users`.`id` IS NULL ORDER BY `phrases_suggestions`.`id`, `phrases_suggestions`.`lang`, `phrases_suggestions`.`author_id` LIMIT 2"
        );
    }

    #[test]
    fn target_lock_covers_only_allowlisted_child_and_parent_tables() {
        let spec = super::super::FkOrphanRepairCase::ArtistsFavorites.spec();

        assert_eq!(
            build_lock_tables_sql("globalcomix", &spec),
            "LOCK TABLES `globalcomix`.`artists_favorites` WRITE, `globalcomix`.`users` READ"
        );
    }

    #[test]
    fn parent_primary_key_comes_from_the_child_foreign_key() {
        let spec = super::super::FkOrphanRepairCase::Comics.spec();
        let child = DatabaseRow {
            primary_key: vec!["9".to_string()],
            values: [
                ("artist_id".to_string(), Some("7".to_string())),
                ("artist_name".to_string(), Some("Current".to_string())),
            ]
            .into_iter()
            .collect(),
        };

        assert_eq!(
            parent_primary_key(&spec, &child, "source child").expect("parent key"),
            vec!["7".to_string()]
        );
    }

    fn sync_table(name: &str, primary_key: &[&str]) -> SyncTable {
        SyncTable {
            name: name.to_string(),
            primary_key: strings(primary_key),
            primary_key_ordering: primary_key
                .iter()
                .map(|_| SyncPrimaryKeyOrdering::Native)
                .collect(),
            columns: strings(primary_key),
            bit_columns: Vec::new(),
            enum_columns: std::collections::BTreeMap::new(),
            mediumblob_columns: Vec::new(),
        }
    }
}
