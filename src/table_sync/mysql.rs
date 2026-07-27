use super::{
    SyncChunkRequest, SyncPrimaryKeyOrdering, SyncTableReader, TableSyncError, UpdatedSince,
};
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
/// Two rows are enough to prove a referenced identity is ambiguous, which the planner rejects.
// Unwired with generic missing-parent deferral (see live::missing_parent); kept for re-enable.
#[allow(dead_code)]
const PARENT_IDENTITY_COLLISION_LIMIT: usize = 2;
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

    pub(crate) fn read_exact_inventory_rows(
        &self,
        table: &crate::inventory::TableInventory,
        identity: &[(String, String)],
    ) -> Result<Vec<SnapshotRow>, TableSyncError> {
        let columns = inventory_stored_columns(table);
        let predicates = identity
            .iter()
            .map(|(column, value)| {
                format!("{} = {}", quote_ident(column), quote_sql_literal(value))
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "SELECT {} FROM {} WHERE {} LIMIT 2",
            quote_ident_list(&columns),
            quote_ident(&table.name),
            predicates
        );
        parse_sync_rows(&columns, &table.primary_key, self.query_rows(&sql)?)
    }

    /// Reads every stored row matching one of `identities` in a single round-trip.
    ///
    /// Verification is round-trip bound, not query bound: a one-row primary-key lookup against the
    /// managed target costs the same as `SELECT 1`. Reading one identity per statement therefore
    /// capped repair throughput at one row per round-trip independent of table size.
    ///
    /// Cardinality is preserved rather than limited to one row per identity, so a caller can still
    /// reject a duplicated identity instead of accepting the first row.
    pub(crate) fn read_exact_inventory_rows_batch(
        &self,
        table: &crate::inventory::TableInventory,
        identities: &[Vec<(String, String)>],
    ) -> Result<Vec<SnapshotRow>, TableSyncError> {
        if identities.is_empty() {
            return Ok(Vec::new());
        }
        let columns = inventory_stored_columns(table);
        let sql = format!(
            "SELECT {} FROM {} WHERE {} LIMIT {}",
            quote_ident_list(&columns),
            quote_ident(&table.name),
            batch_identity_predicate(identities)?,
            identities.len().saturating_mul(2)
        );
        parse_sync_rows(&columns, &table.primary_key, self.query_rows(&sql)?)
    }

    /// Reads the rows owning a referenced foreign-key identity in any parent table.
    ///
    /// Cardinality is preserved up to the collision limit so the planner can reject an ambiguous
    /// identity instead of picking a row.
    // Unwired with generic missing-parent deferral (see live::missing_parent); kept for re-enable.
    #[allow(dead_code)]
    pub(crate) fn read_parent_identity_rows(
        &self,
        table: &crate::inventory::TableInventory,
        columns: &[String],
        values: &[Option<String>],
    ) -> Result<Vec<SnapshotRow>, TableSyncError> {
        let query_columns = inventory_stored_columns(table);
        let sql = format!(
            "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT {}",
            quote_ident_list(&query_columns),
            quote_ident(&table.name),
            parent_identity_predicates(columns, values),
            quote_ident_list(&table.primary_key),
            PARENT_IDENTITY_COLLISION_LIMIT,
        );
        parse_sync_rows(&query_columns, &table.primary_key, self.query_rows(&sql)?)
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

/// Columns an insert can carry, which excludes generated columns the target computes itself.
pub(super) fn inventory_stored_columns(table: &crate::inventory::TableInventory) -> Vec<String> {
    table
        .columns
        .iter()
        .filter(|column| column.generated.is_none())
        .map(|column| column.name.clone())
        .collect()
}

/// A NULL foreign-key value never violates the constraint, so it cannot select a parent by
/// equality. `IS NULL` keeps the read honest and the planner rejects the case outright.
// Unwired with generic missing-parent deferral (see live::missing_parent); kept for re-enable.
#[allow(dead_code)]
fn parent_identity_predicates(columns: &[String], values: &[Option<String>]) -> String {
    columns
        .iter()
        .zip(values)
        .map(|(column, value)| match value {
            Some(value) => format!("{} = {}", quote_ident(column), quote_sql_literal(value)),
            None => format!("{} IS NULL", quote_ident(column)),
        })
        .collect::<Vec<_>>()
        .join(" AND ")
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
    let order_by = primary_key_order_by(&request.primary_key, &request.primary_key_ordering);
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
            &request.primary_key_ordering,
            start_after,
        ));
    }
    if let Some(end_at) = &request.end_at {
        predicates.push(primary_key_at_or_before_predicate(
            &request.primary_key,
            &request.primary_key_ordering,
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

fn primary_key_after_predicate(
    columns: &[String],
    ordering: &[SyncPrimaryKeyOrdering],
    values: &[String],
) -> String {
    primary_key_bound_predicate(columns, ordering, values, ">")
}

fn primary_key_at_or_before_predicate(
    columns: &[String],
    ordering: &[SyncPrimaryKeyOrdering],
    values: &[String],
) -> String {
    format!(
        "NOT ({})",
        primary_key_bound_predicate(columns, ordering, values, ">")
    )
}

fn primary_key_bound_predicate(
    columns: &[String],
    ordering: &[SyncPrimaryKeyOrdering],
    values: &[String],
    operator: &str,
) -> String {
    group_primary_key_bound_branches(
        columns
            .iter()
            .enumerate()
            .map(|(index, _column)| {
                primary_key_bound_branch(columns, ordering, values, index, operator)
            })
            .collect(),
    )
}

/// A multi-column bound is a disjunction, and `AND` binds tighter than `OR`. Without grouping, a
/// second bound combined with `AND` would only constrain the last branch, leaving the window
/// effectively unbounded.
fn group_primary_key_bound_branches(branches: Vec<String>) -> String {
    if branches.len() < 2 {
        return branches.join(" OR ");
    }
    format!("({})", branches.join(" OR "))
}

fn primary_key_bound_branch(
    columns: &[String],
    ordering: &[SyncPrimaryKeyOrdering],
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
    let column = primary_key_order_expression(&columns[index], &ordering[index]);
    let value = primary_key_bound_expression(&values[index], &ordering[index]);
    parts.push(format!("{column} {operator} {value}"));
    format!("({})", parts.join(" AND "))
}

fn primary_key_order_by(columns: &[String], ordering: &[SyncPrimaryKeyOrdering]) -> String {
    columns
        .iter()
        .zip(ordering)
        .map(|(column, ordering)| primary_key_order_expression(column, ordering))
        .collect::<Vec<_>>()
        .join(", ")
}

fn primary_key_order_expression(column: &str, ordering: &SyncPrimaryKeyOrdering) -> String {
    match ordering {
        SyncPrimaryKeyOrdering::Native => quote_ident(column),
        SyncPrimaryKeyOrdering::Enum(labels) => enum_field_expression(&quote_ident(column), labels),
    }
}

fn primary_key_bound_expression(value: &str, ordering: &SyncPrimaryKeyOrdering) -> String {
    match ordering {
        SyncPrimaryKeyOrdering::Native => quote_sql_literal(value),
        SyncPrimaryKeyOrdering::Enum(labels) => {
            enum_field_expression(&quote_sql_literal(value), labels)
        }
    }
}

fn enum_field_expression(value: &str, labels: &[String]) -> String {
    let labels = labels
        .iter()
        .map(|label| quote_sql_literal(label))
        .collect::<Vec<_>>()
        .join(", ");
    format!("FIELD({value}, {labels})")
}

/// Builds a row-constructor `IN` predicate covering every identity in the batch.
///
/// Every identity must name the same columns in the same order, which `row_identity` guarantees by
/// walking the table's primary key. A disagreement means the caller mixed tables, so fail closed
/// rather than emit a predicate that silently matches the wrong rows.
fn batch_identity_predicate(
    identities: &[Vec<(String, String)>],
) -> Result<String, TableSyncError> {
    let Some(first) = identities.first() else {
        return Err(TableSyncError::Repair(
            "batch identity predicate needs at least one identity".to_string(),
        ));
    };
    let columns = first
        .iter()
        .map(|(column, _)| column.clone())
        .collect::<Vec<_>>();
    for identity in identities {
        let names = identity.iter().map(|(column, _)| column.as_str());
        if !names.eq(columns.iter().map(String::as_str)) {
            return Err(TableSyncError::Repair(
                "batch identity columns disagree".to_string(),
            ));
        }
    }
    let tuples = identities
        .iter()
        .map(|identity| {
            let values = identity
                .iter()
                .map(|(_, value)| quote_sql_literal(value))
                .collect::<Vec<_>>()
                .join(",");
            format!("({values})")
        })
        .collect::<Vec<_>>()
        .join(",");
    Ok(format!("({}) IN ({tuples})", quote_ident_list(&columns)))
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

    fn identity(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(column, value)| (column.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn batch_identity_predicate_reads_every_single_column_identity_in_one_statement() {
        let predicate = batch_identity_predicate(&[
            identity(&[("id", "1")]),
            identity(&[("id", "2")]),
            identity(&[("id", "3")]),
        ])
        .expect("single-column batch predicate");

        assert_eq!(predicate, "(`id`) IN (('1'),('2'),('3'))");
    }

    #[test]
    fn batch_identity_predicate_keeps_composite_key_tuples_grouped() {
        let predicate = batch_identity_predicate(&[
            identity(&[("comic_id", "10279"), ("subscriber_id", "371917")]),
            identity(&[("comic_id", "10280"), ("subscriber_id", "8835")]),
        ])
        .expect("composite batch predicate");

        assert_eq!(
            predicate,
            "(`comic_id`, `subscriber_id`) IN (('10279','371917'),('10280','8835'))"
        );
    }

    #[test]
    fn batch_identity_predicate_escapes_quoted_identity_values() {
        let predicate =
            batch_identity_predicate(&[identity(&[("name", "O'Brien")])]).expect("escaped value");

        assert_eq!(predicate, "(`name`) IN (('O''Brien'))");
    }

    #[test]
    fn batch_identity_predicate_rejects_identities_naming_different_columns() {
        let error = batch_identity_predicate(&[
            identity(&[("id", "1")]),
            identity(&[("comic_id", "1"), ("subscriber_id", "2")]),
        ])
        .expect_err("mixed identity columns must fail closed");

        assert!(
            error
                .to_string()
                .contains("batch identity columns disagree")
        );
    }

    #[test]
    fn batch_identity_predicate_rejects_an_empty_batch() {
        let error = batch_identity_predicate(&[]).expect_err("empty batch has no predicate");

        assert!(error.to_string().contains("at least one identity"));
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
