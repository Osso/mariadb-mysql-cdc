use super::model::*;
use super::schema::*;
use super::sql::*;
use crate::mysql_support::quote_sql_literal;
use mysql::Conn;
use mysql::prelude::Queryable;
use std::cell::RefCell;

pub struct MySqlConflictLedger {
    conn: RefCell<Conn>,
    table: String,
}

impl MySqlConflictLedger {
    pub fn new(
        target: &crate::live::TargetMySqlConfig,
        table: impl Into<String>,
    ) -> Result<Self, String> {
        let table = table.into();
        let options = crate::mysql_support::target_mysql_opts(target)?;
        let mut conn = Conn::new(options)
            .map_err(|error| format!("conflict ledger connect failed: {error}"))?;
        conn.query_drop(crate::live::target_session_init_command())
            .map_err(|error| format!("conflict ledger session initialization failed: {error}"))?;
        Ok(Self {
            conn: RefCell::new(conn),
            table,
        })
    }

    pub fn ensure(&self) -> Result<(), String> {
        let (schema, table) = split_conflict_table(&self.table)?;
        let mut conn = self.conn.borrow_mut();
        validate_conflict_columns(&query_conflict_columns(&mut conn, schema, table)?)
            .map_err(conflict_validation_error)?;
        validate_conflict_identity_definition(&query_identity_definition(
            &mut conn, schema, table,
        )?)
        .map_err(conflict_validation_error)?;
        validate_source_row_identity_definition(&query_source_row_identity_definition(
            &mut conn, schema, table,
        )?)
        .map_err(conflict_validation_error)?;
        validate_conflict_keys(&query_conflict_keys(&mut conn, schema, table)?)
            .map_err(conflict_validation_error)?;
        validate_conflict_constraints(&query_conflict_constraints(&mut conn, schema, table)?)
            .map_err(conflict_validation_error)?;
        validate_conflict_status_checks(&query_conflict_checks(&mut conn, schema, table)?)
            .map_err(conflict_validation_error)?;
        let triggers = query_conflict_trigger_inventory(&mut conn, &self.table)?;
        validate_conflict_triggers(schema, table, &triggers).map_err(conflict_validation_error)
    }

    pub(crate) fn unresolved_source_rows(
        &self,
        source_identity: &str,
        schema: &str,
        tables: &[String],
        limit: usize,
    ) -> Result<Vec<ConflictKey>, String> {
        if tables.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let table_values = tables
            .iter()
            .map(|table| quote_sql_literal(table))
            .collect::<Vec<_>>()
            .join(",");
        let query = format!(
            "SELECT conflict_identity,source_row_identity,source_identity,source_server_id,source_file,source_start_position,schema_name,table_name,operation,source_primary_key_json FROM {} WHERE status='unresolved' AND source_identity={} AND schema_name={} AND table_name IN ({}) ORDER BY first_observed_at_ms,conflict_identity LIMIT {}",
            self.table,
            quote_sql_literal(source_identity),
            quote_sql_literal(schema),
            table_values,
            limit,
        );
        let rows = self
            .conn
            .borrow_mut()
            .query::<ConflictIdentityRow, _>(query)
            .map_err(|error| format!("conflict candidate read failed: {error}"))?;
        rows.into_iter()
            .map(|row| {
                validate_conflict_identity_row(&row)?;
                conflict_key_from_identity_row(&row)
            })
            .collect()
    }

    pub fn resolve_verified_table(
        &mut self,
        source_identity: &str,
        table: &str,
        repair_run_id: &str,
        evidence: &str,
    ) -> Result<(), String> {
        self.validate_unresolved_rows(Some(source_identity), Some(table))?;
        self.conn
            .borrow_mut()
            .query_drop(build_conflict_table_resolution_sql(
                &self.table,
                source_identity,
                table,
                repair_run_id,
                evidence,
            ))
            .map_err(|error| format!("conflict ledger table resolution failed: {error}"))?;
        Ok(())
    }

    fn validate_unresolved_rows(
        &self,
        source_identity: Option<&str>,
        table: Option<&str>,
    ) -> Result<(), String> {
        let mut query = format!(
            "SELECT conflict_identity,source_row_identity,source_identity,source_server_id,source_file,source_start_position,schema_name,table_name,operation,source_primary_key_json FROM {} WHERE status='unresolved'",
            self.table
        );
        if let Some(source_identity) = source_identity {
            query.push_str(" AND source_identity=");
            query.push_str(&quote_sql_literal(source_identity));
        }
        if let Some(table) = table {
            query.push_str(" AND table_name=");
            query.push_str(&quote_sql_literal(table));
        }
        let rows = self
            .conn
            .borrow_mut()
            .query::<ConflictIdentityRow, _>(query)
            .map_err(|error| format!("conflict identity read failed: {error}"))?;
        for row in rows {
            validate_conflict_identity_row(&row)?;
        }
        Ok(())
    }
}

fn query_conflict_columns(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<ConflictColumn>, String> {
    conn.query(format!(
        "SELECT column_name,LOWER(column_type),is_nullable,LOWER(COALESCE(CAST(column_default AS CHAR),'<null>')),LOWER(extra) FROM information_schema.columns WHERE table_schema={} AND table_name={} ORDER BY ordinal_position",
        quote_sql_literal(schema), quote_sql_literal(table),
    )).map_err(conflict_mysql_error)
}

fn query_identity_definition(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<ConflictIdentityDefinition, String> {
    conn.query_first(format!(
        "SELECT LOWER(COALESCE(character_set_name,'')),LOWER(COALESCE(collation_name,'')) FROM information_schema.columns WHERE table_schema={} AND table_name={} AND column_name='conflict_identity'",
        quote_sql_literal(schema), quote_sql_literal(table),
    )).map_err(conflict_mysql_error)?.ok_or_else(|| "conflict identity column definition is missing".to_string())
}

fn query_source_row_identity_definition(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<SourceRowIdentityDefinition, String> {
    conn.query_first(format!(
        "SELECT LOWER(COALESCE(character_set_name,'')),LOWER(COALESCE(collation_name,'')),generation_expression FROM information_schema.columns WHERE table_schema={} AND table_name={} AND column_name='source_row_identity'",
        quote_sql_literal(schema), quote_sql_literal(table),
    ))
    .map_err(conflict_mysql_error)?
    .ok_or_else(|| "source row identity column definition is missing".to_string())
}

fn query_conflict_keys(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<ConflictKeyIndex>, String> {
    conn.query(format!(
        "SELECT index_name,non_unique,seq_in_index,column_name,sub_part FROM information_schema.statistics WHERE table_schema={} AND table_name={} ORDER BY index_name,seq_in_index",
        quote_sql_literal(schema), quote_sql_literal(table),
    )).map_err(conflict_mysql_error)
}

fn query_conflict_constraints(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<ConflictConstraint>, String> {
    conn.query(format!(
        "SELECT constraint_type,enforced FROM information_schema.table_constraints WHERE table_schema={} AND table_name={} ORDER BY constraint_type,constraint_name",
        quote_sql_literal(schema), quote_sql_literal(table),
    )).map_err(conflict_mysql_error)
}

fn query_conflict_checks(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, String> {
    conn.query(format!(
        "SELECT cc.check_clause FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON cc.constraint_schema=tc.constraint_schema AND cc.constraint_name=tc.constraint_name WHERE tc.table_schema={} AND tc.table_name={} AND tc.constraint_type='CHECK' ORDER BY tc.constraint_name",
        quote_sql_literal(schema), quote_sql_literal(table),
    )).map_err(conflict_mysql_error)
}

impl MySqlConflictLedger {
    pub fn resolve_existing(&mut self, resolution: ConflictResolution) -> Result<(), String> {
        self.conn
            .borrow_mut()
            .query_drop(build_conflict_resolution_for_source_row_sql(
                &self.table,
                &resolution,
            ))
            .map_err(|error| format!("conflict ledger resolution failed: {error}"))?;
        Ok(())
    }

    pub fn resolve_if_equal(
        &mut self,
        table: &str,
        primary_key: &[String],
        rows_equal: bool,
        repair_run_id: &str,
        evidence: &str,
    ) -> Result<(), String> {
        if !rows_equal {
            return Ok(());
        }
        self.conn
            .borrow_mut()
            .query_drop(build_conflict_resolution_by_table_sql(
                &self.table,
                table,
                primary_key,
                repair_run_id,
                evidence,
            ))
            .map_err(|error| format!("conflict ledger resolution failed: {error}"))?;
        Ok(())
    }
}
