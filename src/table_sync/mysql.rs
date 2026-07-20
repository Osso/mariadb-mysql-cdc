use super::{SyncChunkRequest, SyncTableReader, TableSyncError, UpdatedSince};
use crate::live::TargetMySqlConfig;
use crate::mysql_client::{PersistentMySqlSource, target_reader_opts};
use crate::snapshot::{SnapshotError, SnapshotRow};
use std::cell::RefCell;
use std::collections::BTreeMap;

pub(super) const GUEST_COLUMNS: [&str; 23] = [
    "guest_id",
    "guest_hash",
    "country",
    "original_ref",
    "original_uri",
    "first_user_id",
    "geo_region_id",
    "ui_lang",
    "device_type",
    "et_id",
    "utm_medium",
    "utm_source",
    "utm_campaign",
    "utm_term",
    "utm_id",
    "http_user_agent",
    "create_time",
    "is_bot",
    "params",
    "application_user_access_token_id",
    "application_id",
    "supports_cookies",
    "reason",
];
pub(super) const HOME_FEED_CARD_COLUMNS: [&str; 20] = [
    "id",
    "card_type_id",
    "status",
    "reading_direction",
    "comic_id",
    "release_id",
    "caption",
    "hook_image_url",
    "source_id",
    "filter_reason",
    "retired_reason",
    "first_published",
    "last_active_time",
    "view_count",
    "reaction_count",
    "click_count",
    "curator_user_id",
    "curated_score",
    "facets_json",
    "create_time",
];
pub(super) const RECOVERY_CREATE_TIME_EPOCH_ALIAS: &str = "__recovery_create_time_epoch";
const GUEST_IDENTITY_COLLISION_LIMIT: usize = 3;
const HOME_FEED_CARD_IDENTITY_COLLISION_LIMIT: usize = 3;
pub(super) const RECOVERY_UTC_SESSION_SQL: &str = "SET SESSION time_zone='+00:00'";

pub(crate) struct MySqlSyncReader {
    config: crate::mysql_snapshot::MySqlConnectionConfig,
    tls_ca_file: Option<String>,
    source: RefCell<Option<PersistentMySqlSource>>,
    target_opts: Option<mysql::Opts>,
    replace_divergent_primary: bool,
    initialize_recovery_utc: bool,
}

impl MySqlSyncReader {
    pub fn new(config: crate::mysql_snapshot::MySqlConnectionConfig) -> Self {
        Self::new_with_tls_ca(config, None)
    }

    pub(crate) fn new_with_tls_ca(
        config: crate::mysql_snapshot::MySqlConnectionConfig,
        tls_ca_file: Option<String>,
    ) -> Self {
        Self {
            config,
            tls_ca_file,
            source: RefCell::new(None),
            target_opts: None,
            replace_divergent_primary: false,
            initialize_recovery_utc: false,
        }
    }

    pub(crate) fn new_with_target(
        config: crate::mysql_snapshot::MySqlConnectionConfig,
        target: &TargetMySqlConfig,
    ) -> Result<Self, String> {
        Ok(Self {
            config,
            tls_ca_file: None,
            source: RefCell::new(None),
            target_opts: Some(target_reader_opts(target)?),
            replace_divergent_primary: target.insert_conflict_policy
                == crate::live::InsertConflictPolicy::ReplaceDivergentPk,
            initialize_recovery_utc: false,
        })
    }

    pub(crate) fn with_recovery_utc(mut self) -> Self {
        self.initialize_recovery_utc = true;
        self
    }

    pub(crate) fn read_guest_identity_rows(
        &self,
        guest_id: &str,
        guest_hash: &str,
    ) -> Result<Vec<SnapshotRow>, TableSyncError> {
        let mut query_columns = guest_columns();
        query_columns.push(RECOVERY_CREATE_TIME_EPOCH_ALIAS.to_string());
        let sql = build_guest_identity_sql(guest_id, guest_hash);
        parse_sync_rows(
            &query_columns,
            &["guest_id".to_string()],
            self.query_rows(&sql)?,
        )
    }

    pub(crate) fn read_home_feed_card_rows_by_id(
        &self,
        card_id: &str,
    ) -> Result<Vec<SnapshotRow>, TableSyncError> {
        let sql = build_home_feed_card_id_sql(card_id);
        parse_home_feed_card_rows(self.query_rows(&sql)?)
    }

    pub(crate) fn read_home_feed_card_identity_rows(
        &self,
        card_id: &str,
        card_type_id: &str,
        source_id: Option<&str>,
    ) -> Result<Vec<SnapshotRow>, TableSyncError> {
        let sql = build_home_feed_card_identity_sql(card_id, card_type_id, source_id);
        parse_home_feed_card_rows(self.query_rows(&sql)?)
    }

    fn query_rows(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>, TableSyncError> {
        self.connect_source_if_needed()?
            .query_rows_as_strings(sql)
            .map_err(snapshot_error_to_table_sync)
    }

    fn connect_source_if_needed(
        &self,
    ) -> Result<std::cell::RefMut<'_, PersistentMySqlSource>, TableSyncError> {
        if self.source.borrow().is_none() {
            let source = match &self.target_opts {
                Some(opts) => PersistentMySqlSource::new_with_opts(opts.clone()),
                None => PersistentMySqlSource::new_with_tls_ca(
                    &self.config,
                    self.tls_ca_file.as_deref(),
                ),
            }
            .map_err(snapshot_error_to_table_sync)?;
            initialize_recovery_session(self.initialize_recovery_utc, || {
                source
                    .execute_session_sql(RECOVERY_UTC_SESSION_SQL)
                    .map_err(snapshot_error_to_table_sync)
            })?;
            self.source.replace(Some(source));
        }
        Ok(std::cell::RefMut::map(self.source.borrow_mut(), |source| {
            source.as_mut().expect("sync source initialized")
        }))
    }
}

impl SyncTableReader for MySqlSyncReader {
    fn read_rows(&self, request: &SyncChunkRequest) -> Result<Vec<SnapshotRow>, TableSyncError> {
        let sql = build_sync_select_sql(request);
        let rows = self.query_rows(&sql)?;
        parse_sync_rows(&request.columns, &request.primary_key, rows)
    }

    fn requires_full_rows_for_missing_primary_keys(&self) -> bool {
        self.replace_divergent_primary
    }
}

fn initialize_recovery_session<F>(enabled: bool, initialize: F) -> Result<(), TableSyncError>
where
    F: FnOnce() -> Result<(), TableSyncError>,
{
    if enabled {
        initialize()?;
    }
    Ok(())
}

fn build_guest_identity_sql(guest_id: &str, guest_hash: &str) -> String {
    format!(
        "SELECT {}, UNIX_TIMESTAMP(`create_time`) AS {} FROM `guests` WHERE `guest_id` = {} OR `guest_hash` = {} ORDER BY `guest_id` LIMIT {}",
        quote_ident_list(&guest_columns()),
        quote_ident(RECOVERY_CREATE_TIME_EPOCH_ALIAS),
        quote_sql_literal(guest_id),
        quote_sql_literal(guest_hash),
        GUEST_IDENTITY_COLLISION_LIMIT,
    )
}

fn build_home_feed_card_id_sql(card_id: &str) -> String {
    format!(
        "SELECT {}, UNIX_TIMESTAMP(`create_time`) AS {} FROM `home_feed_cards` WHERE `id` = {} ORDER BY `id` LIMIT {}",
        quote_ident_list(&home_feed_card_columns()),
        quote_ident(RECOVERY_CREATE_TIME_EPOCH_ALIAS),
        quote_sql_literal(card_id),
        HOME_FEED_CARD_IDENTITY_COLLISION_LIMIT,
    )
}

fn build_home_feed_card_identity_sql(
    card_id: &str,
    card_type_id: &str,
    source_id: Option<&str>,
) -> String {
    let unique_collision = source_id.map(|source_id| {
        format!(
            " OR (`card_type_id` = {} AND `source_id` = {})",
            quote_sql_literal(card_type_id),
            quote_sql_literal(source_id),
        )
    });
    format!(
        "SELECT {}, UNIX_TIMESTAMP(`create_time`) AS {} FROM `home_feed_cards` WHERE `id` = {}{} ORDER BY `id` LIMIT {}",
        quote_ident_list(&home_feed_card_columns()),
        quote_ident(RECOVERY_CREATE_TIME_EPOCH_ALIAS),
        quote_sql_literal(card_id),
        unique_collision.unwrap_or_default(),
        HOME_FEED_CARD_IDENTITY_COLLISION_LIMIT,
    )
}

fn parse_home_feed_card_rows(
    rows: Vec<Vec<Option<String>>>,
) -> Result<Vec<SnapshotRow>, TableSyncError> {
    let mut query_columns = home_feed_card_columns();
    query_columns.push(RECOVERY_CREATE_TIME_EPOCH_ALIAS.to_string());
    parse_sync_rows(&query_columns, &["id".to_string()], rows)
}

pub(super) fn home_feed_card_columns() -> Vec<String> {
    HOME_FEED_CARD_COLUMNS
        .iter()
        .map(|column| (*column).to_string())
        .collect()
}

pub(super) fn guest_columns() -> Vec<String> {
    GUEST_COLUMNS
        .iter()
        .map(|column| (*column).to_string())
        .collect()
}

fn snapshot_error_to_table_sync(error: SnapshotError) -> TableSyncError {
    TableSyncError::Read(error.to_string())
}

pub(crate) fn build_sync_select_sql(request: &SyncChunkRequest) -> String {
    let columns = quote_ident_list(&request.columns);
    let order_by = quote_ident_list(&request.primary_key);
    let bounds = sync_bounds(request);
    format!(
        "SELECT {columns} FROM {}{bounds} ORDER BY {order_by} LIMIT {}",
        quote_ident(&request.table),
        request.limit
    )
}

fn sync_bounds(request: &SyncChunkRequest) -> String {
    let predicates = sync_bound_predicates(request);
    if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    }
}

fn sync_bound_predicates(request: &SyncChunkRequest) -> Vec<String> {
    let mut predicates = Vec::new();
    if let Some(start_after) = &request.start_after {
        predicates.push(primary_key_after_predicate(
            &request.primary_key,
            start_after,
        ));
    }
    if let Some(end_at) = &request.end_at {
        predicates.push(primary_key_at_or_before_predicate(
            &request.primary_key,
            end_at,
        ));
    }
    if let Some(updated_since) = &request.updated_since {
        predicates.push(updated_since_predicate(updated_since));
    }
    predicates
}

fn updated_since_predicate(updated_since: &UpdatedSince) -> String {
    format!(
        "{} >= {}",
        quote_ident(&updated_since.column),
        quote_sql_literal(&updated_since.value)
    )
}

fn parse_sync_rows(
    columns: &[String],
    primary_key: &[String],
    rows: Vec<Vec<Option<String>>>,
) -> Result<Vec<SnapshotRow>, TableSyncError> {
    rows.into_iter()
        .map(|fields| parse_sync_row(columns, primary_key, fields))
        .collect()
}

fn parse_sync_row(
    columns: &[String],
    primary_key: &[String],
    fields: Vec<Option<String>>,
) -> Result<SnapshotRow, TableSyncError> {
    if fields.len() != columns.len() {
        return Err(TableSyncError::Read(format!(
            "sync row has {} fields for {} columns",
            fields.len(),
            columns.len()
        )));
    }

    let values = columns
        .iter()
        .cloned()
        .zip(fields)
        .collect::<BTreeMap<_, _>>();
    let primary_key = primary_key_values(primary_key, &values)?;
    Ok(SnapshotRow {
        primary_key,
        values,
    })
}

fn primary_key_values(
    primary_key: &[String],
    values: &BTreeMap<String, Option<String>>,
) -> Result<Vec<String>, TableSyncError> {
    primary_key
        .iter()
        .map(|column| {
            let value = values.get(column).cloned().ok_or_else(|| {
                TableSyncError::Read(format!("primary key column `{column}` missing from row"))
            })?;
            value.ok_or_else(|| {
                TableSyncError::Read(format!("primary key column `{column}` was NULL"))
            })
        })
        .collect()
}

fn primary_key_after_predicate(columns: &[String], values: &[String]) -> String {
    primary_key_bound_predicate(columns, values, ">")
}

fn primary_key_at_or_before_predicate(columns: &[String], values: &[String]) -> String {
    format!(
        "NOT ({})",
        primary_key_bound_predicate(columns, values, ">")
    )
}

fn primary_key_bound_predicate(columns: &[String], values: &[String], operator: &str) -> String {
    columns
        .iter()
        .enumerate()
        .map(|(index, _column)| primary_key_bound_branch(columns, values, index, operator))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn primary_key_bound_branch(
    columns: &[String],
    values: &[String],
    index: usize,
    operator: &str,
) -> String {
    let mut parts = Vec::new();
    for equal_index in 0..index {
        parts.push(format!(
            "{} = {}",
            quote_ident(&columns[equal_index]),
            quote_sql_literal(&values[equal_index])
        ));
    }
    parts.push(format!(
        "{} {operator} {}",
        quote_ident(&columns[index]),
        quote_sql_literal(&values[index])
    ));
    format!("({})", parts.join(" AND "))
}

fn quote_ident_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns() -> Vec<String> {
        vec![
            "id".to_string(),
            "artist_id".to_string(),
            "name".to_string(),
        ]
    }

    #[test]
    fn recovery_session_initialization_runs_once_per_connection() {
        let initialization_count = RefCell::new(0);
        initialize_recovery_session(true, || {
            *initialization_count.borrow_mut() += 1;
            Ok(())
        })
        .expect("initialize recovery connection");

        let guest_query = build_guest_identity_sql("1", "hash");
        let generic_queries = ["SELECT 1", guest_query.as_str()];
        assert_eq!(generic_queries.len(), 2);
        assert_eq!(*initialization_count.borrow(), 1);
    }

    #[test]
    fn guest_identity_query_returns_canonical_columns_with_absolute_epoch_helper() {
        let sql = build_guest_identity_sql("78011674", "guest-hash");

        assert!(sql.contains("UNIX_TIMESTAMP(`create_time`) AS `__recovery_create_time_epoch`"));
        assert!(sql.starts_with("SELECT `guest_id`, `guest_hash`,"));
    }

    #[test]
    fn home_feed_card_identity_query_checks_primary_and_non_null_unique_owner() {
        let sql = build_home_feed_card_identity_sql("2492683", "1", Some("50151"));

        assert!(sql.contains("FROM `home_feed_cards`"));
        assert!(sql.contains("`id` = '2492683'"));
        assert!(sql.contains("`card_type_id` = '1' AND `source_id` = '50151'"));
        assert!(sql.contains("UNIX_TIMESTAMP(`create_time`)"));
        assert_eq!(home_feed_card_columns().len(), 20);

        let null_source_sql = build_home_feed_card_identity_sql("2492683", "1", None);
        assert!(!null_source_sql.contains("`card_type_id` ="));
    }

    #[test]
    fn preserves_null_values_and_rejects_null_primary_keys() {
        let rows = parse_sync_rows(
            &columns(),
            &["id".to_string()],
            vec![vec![Some("1".to_string()), None, Some("NULL".to_string())]],
        )
        .expect("row");

        assert_eq!(rows[0].values["artist_id"], None);
        assert_eq!(rows[0].values["name"], Some("NULL".to_string()));

        let error = parse_sync_rows(
            &columns(),
            &["id".to_string()],
            vec![vec![None, Some("2".to_string()), Some("name".to_string())]],
        )
        .expect_err("null primary key");
        assert_eq!(
            error.to_string(),
            "sync read failed: primary key column `id` was NULL"
        );
    }
}
