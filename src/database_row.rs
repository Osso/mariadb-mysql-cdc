use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DatabaseRow {
    pub primary_key: Vec<String>,
    pub values: BTreeMap<String, Option<String>>,
}
