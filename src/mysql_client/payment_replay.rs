use super::{PersistentMySqlSource, PersistentTargetExecutor};
use crate::mysql_support::quote_ident;
use crate::target::{TargetExecuteError, TargetRowChange, TargetRowChangeKind};
use mysql::prelude::Queryable;
use mysql::{Conn, Params, Row, Value};

const PAYMENT_GUARD: &str = "This external payment has already been applied to a previous order";
const EXTERNAL_IDENTITY: [&str; 4] = [
    "payment_service_id",
    "transaction_id",
    "authorization_id",
    "original_transaction_id",
];

fn numeric(value: &Value) -> Option<u64> {
    match value {
        Value::UInt(value) => Some(*value),
        Value::Int(value) => u64::try_from(*value).ok(),
        _ => None,
    }
}

fn same_value(left: &Value, right: &Value) -> bool {
    match (numeric(left), numeric(right)) {
        (Some(left), Some(right)) => left == right,
        _ => left == right,
    }
}

fn candidate_id(change: &TargetRowChange, error: &TargetExecuteError) -> Option<u64> {
    if change.kind != TargetRowChangeKind::Insert
        || change.table != "payments"
        || error.mysql_code() != Some(1644)
        || !error.to_string().contains(PAYMENT_GUARD)
        || !matches!(
            change.values.get("payment_service_id").and_then(numeric),
            Some(8 | 9)
        )
    {
        return None;
    }
    for column in EXTERNAL_IDENTITY
        .into_iter()
        .chain(["order_id", "owner_type_id", "owner_id"])
    {
        if !change.values.contains_key(column) {
            return None;
        }
    }
    change.values.get("id").and_then(numeric)
}

fn projection(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(",")
}

fn qualified_table(change: &TargetRowChange) -> String {
    format!(
        "{}.{}",
        quote_ident(&change.schema),
        quote_ident(&change.table)
    )
}

fn lock_existing_payment(
    target: &mut Conn,
    change: &TargetRowChange,
    columns: &[String],
    id: u64,
) -> Result<Option<Vec<Value>>, TargetExecuteError> {
    let predicates = EXTERNAL_IDENTITY
        .iter()
        .map(|column| format!("{} <=> ?", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let params = EXTERNAL_IDENTITY
        .iter()
        .map(|column| change.values[*column].clone())
        .collect::<Vec<_>>();
    let owners: Vec<Row> = target
        .exec(
            format!(
                "SELECT {} FROM {} WHERE {predicates} LIMIT 2 FOR UPDATE",
                projection(columns),
                qualified_table(change)
            ),
            Params::Positional(params),
        )
        .map_err(|error| {
            TargetExecuteError::new(format!("payment replay target proof query failed: {error}"))
        })?;
    if owners.len() != 1 {
        eprintln!(
            "cdc_payment_replay_probe id={id} target_owner_count={} verified=false",
            owners.len()
        );
        return Ok(None);
    }
    let values = owners.into_iter().next().expect("one owner").unwrap();
    let id_index = columns
        .iter()
        .position(|column| column == "id")
        .expect("validated id");
    if values.get(id_index).and_then(numeric) != Some(id) {
        eprintln!("cdc_payment_replay_probe id={id} target_id_matches=false verified=false");
        return Ok(None);
    }
    Ok(Some(values))
}

fn read_current_payment(
    source: &PersistentMySqlSource,
    change: &TargetRowChange,
    columns: &[String],
    id: u64,
) -> Result<Option<Vec<Value>>, TargetExecuteError> {
    let rows: Vec<Row> = source
        .conn
        .borrow_mut()
        .exec(
            format!(
                "SELECT {} FROM {} WHERE `id` = ? LIMIT 2",
                projection(columns),
                qualified_table(change)
            ),
            (id,),
        )
        .map_err(|error| {
            TargetExecuteError::new(format!("payment replay source proof query failed: {error}"))
        })?;
    if rows.len() != 1 {
        eprintln!("cdc_payment_replay_probe id={id} source_row_present=false verified=false");
        return Ok(None);
    }
    Ok(Some(
        rows.into_iter().next().expect("one source row").unwrap(),
    ))
}

fn verify_payment_values(
    change: &TargetRowChange,
    columns: &[String],
    target: &[Value],
    source: &[Value],
    id: u64,
) -> Result<bool, TargetExecuteError> {
    if target.len() != columns.len() || source.len() != columns.len() {
        return Err(TargetExecuteError::new(
            "payment replay proof column count differs",
        ));
    }
    let identity_matches = EXTERNAL_IDENTITY.iter().all(|name| {
        let index = columns
            .iter()
            .position(|column| column == name)
            .expect("validated identity column");
        same_value(&source[index], &change.values[*name])
    });
    let rows_match = target
        .iter()
        .zip(source)
        .all(|(target, source)| same_value(target, source));
    let verified = identity_matches && rows_match;
    eprintln!(
        "cdc_payment_replay_probe id={id} source_identity_matches={identity_matches} source_row_matches={rows_match} verified={verified}"
    );
    Ok(verified)
}

impl PersistentTargetExecutor {
    pub(super) fn prove_existing_payment_replay(
        &self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<bool, TargetExecuteError> {
        if !self.payment_replay_enabled {
            return Ok(false);
        }
        let Some(id) = candidate_id(change, error) else {
            return Ok(false);
        };
        let Some(source) = self.source.as_ref() else {
            return Ok(false);
        };
        let columns = change.values.keys().cloned().collect::<Vec<_>>();
        self.with_connection(|target| {
            let Some(target_values) = lock_existing_payment(target, change, &columns, id)? else {
                return Ok(false);
            };
            let Some(current_values) = read_current_payment(source, change, &columns, id)? else {
                return Ok(false);
            };
            verify_payment_values(change, &columns, &target_values, &current_values, id)
        })
    }
}
