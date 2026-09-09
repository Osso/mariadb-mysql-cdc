use super::PersistentTargetExecutor;
use crate::mysql_support::quote_ident;
use crate::target::{TargetExecuteError, TargetRowChange, TargetRowChangeKind};
use mysql::prelude::Queryable;
use mysql::{Params, Row, Value};

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

impl PersistentTargetExecutor {
    pub(super) fn prove_existing_payment_replay(
        &self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<bool, TargetExecuteError> {
        if !self.payment_replay_enabled
            || change.kind != TargetRowChangeKind::Insert
            || change.table != "payments"
            || error.mysql_code() != Some(1644)
            || !error.to_string().contains(PAYMENT_GUARD)
        {
            return Ok(false);
        }
        let Some(id) = change.values.get("id").and_then(numeric) else {
            return Ok(false);
        };
        if !matches!(
            change.values.get("payment_service_id").and_then(numeric),
            Some(8 | 9)
        ) {
            return Ok(false);
        }
        for column in EXTERNAL_IDENTITY
            .into_iter()
            .chain(["order_id", "owner_type_id", "owner_id"])
        {
            if !change.values.contains_key(column) {
                return Ok(false);
            }
        }
        let Some(source) = self.source.as_ref() else {
            return Ok(false);
        };
        let columns = change.values.keys().cloned().collect::<Vec<_>>();
        let projection = columns
            .iter()
            .map(|column| quote_ident(column))
            .collect::<Vec<_>>()
            .join(",");
        let table = format!(
            "{}.{}",
            quote_ident(&change.schema),
            quote_ident(&change.table)
        );
        let predicates = EXTERNAL_IDENTITY
            .iter()
            .map(|column| format!("{} <=> ?", quote_ident(column)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let params = EXTERNAL_IDENTITY
            .iter()
            .map(|column| change.values[*column].clone())
            .collect::<Vec<_>>();
        let id_index = columns
            .iter()
            .position(|column| column == "id")
            .expect("validated id");
        self.with_connection(|target| {
            let owners: Vec<Row> = target.exec(
                format!("SELECT {projection} FROM {table} WHERE {predicates} LIMIT 2 FOR UPDATE"),
                Params::Positional(params),
            ).map_err(|error| TargetExecuteError::new(format!("payment replay target proof query failed: {error}")))?;
            if owners.len() != 1 {
                eprintln!("cdc_payment_replay_probe id={id} target_owner_count={} verified=false", owners.len());
                return Ok(false);
            }
            let target_values = owners.into_iter().next().expect("one owner").unwrap();
            if target_values.get(id_index).and_then(numeric) != Some(id) {
                eprintln!("cdc_payment_replay_probe id={id} target_id_matches=false verified=false");
                return Ok(false);
            }
            let current: Vec<Row> = source.conn.borrow_mut().exec(
                format!("SELECT {projection} FROM {table} WHERE `id` = ? LIMIT 2"),
                (id,),
            ).map_err(|error| TargetExecuteError::new(format!("payment replay source proof query failed: {error}")))?;
            if current.len() != 1 {
                eprintln!("cdc_payment_replay_probe id={id} source_row_present=false verified=false");
                return Ok(false);
            }
            let current_values = current.into_iter().next().expect("one source row").unwrap();
            let identity_matches = EXTERNAL_IDENTITY.iter().all(|name| {
                let index = columns.iter().position(|column| column == name).expect("validated identity column");
                same_value(&current_values[index], &change.values[*name])
            });
            let rows_match = target_values.iter().zip(&current_values).all(|(target, source)| same_value(target, source));
            let verified = identity_matches && rows_match;
            eprintln!("cdc_payment_replay_probe id={id} source_identity_matches={identity_matches} source_row_matches={rows_match} verified={verified}");
            Ok(verified)
        })
    }
}
