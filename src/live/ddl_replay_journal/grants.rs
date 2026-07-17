use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Eq, PartialEq)]
struct GrantRecord {
    privileges: Vec<String>,
    scope: String,
    grant_option: bool,
    role_or_proxy: bool,
}

fn normalized_grant(grant: &str) -> String {
    grant.replace('`', "").to_ascii_uppercase()
}

fn grant_body(grant: &str) -> Result<String, String> {
    normalized_grant(grant)
        .strip_prefix("GRANT ")
        .map(str::to_string)
        .ok_or_else(|| format!("unsupported SHOW GRANTS row: {grant}"))
}

fn grant_scope(scope_and_recipient: &str) -> String {
    scope_and_recipient
        .split_once(" TO ")
        .map(|(scope, _)| scope.trim())
        .unwrap_or(scope_and_recipient.trim())
        .to_string()
}

fn grant_privileges(privilege_text: &str) -> Vec<String> {
    privilege_text
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_grant_body(body: &str) -> GrantRecord {
    let grant_option = body.contains(" WITH GRANT OPTION");
    let role_or_proxy = !body.contains(" ON ") || body.contains(" PROXY ");
    let Some((privilege_text, scope_and_recipient)) = body.split_once(" ON ") else {
        return GrantRecord {
            privileges: vec![],
            scope: String::new(),
            grant_option,
            role_or_proxy: true,
        };
    };
    GrantRecord {
        privileges: grant_privileges(privilege_text),
        scope: grant_scope(scope_and_recipient),
        grant_option,
        role_or_proxy,
    }
}

fn parse_effective_grant(grant: &str) -> Result<GrantRecord, String> {
    Ok(parse_grant_body(&grant_body(grant)?))
}

const APPLICATION_DML_PRIVILEGES: &[&str] = &["SELECT", "INSERT", "UPDATE", "DELETE"];
const APPLICATION_DDL_PRIVILEGES: &[&str] = &[
    "CREATE",
    "ALTER",
    "DROP",
    "INDEX",
    "REFERENCES",
    "CREATE VIEW",
    "SHOW VIEW",
    "CREATE ROUTINE",
    "ALTER ROUTINE",
    "EXECUTE",
    "EVENT",
    "TRIGGER",
];
const CHECKPOINT_PRIVILEGES: &[&str] = &["SELECT", "INSERT", "UPDATE"];
const JOURNAL_PRIVILEGES: &[&str] = &["SELECT", "INSERT", "UPDATE"];
const EXECUTE_PRIVILEGES: &[&str] = &["EXECUTE"];

#[derive(Clone, Copy)]
enum RuntimeGrantScope {
    Application,
    Control(&'static [&'static str]),
}

// Runtime grants are validated once by journal.ensure during startup, not per event.
pub(crate) fn validate_runtime_grants(
    grants: &[String],
    application_schema: &str,
    checkpoint_table: &str,
    journal_table: &str,
    conflict_table: &str,
    inventory_procedure: &str,
) -> Result<(), String> {
    let policy = RuntimeGrantPolicy::new(
        application_schema,
        checkpoint_table,
        journal_table,
        conflict_table,
        inventory_procedure,
    );
    let by_scope = collect_grants(grants, &policy)?;
    validate_required_runtime_scopes(&by_scope, &policy)
}

fn collect_grants(
    grants: &[String],
    policy: &RuntimeGrantPolicy,
) -> Result<HashMap<String, HashSet<String>>, String> {
    let mut by_scope = HashMap::new();
    for grant in grants {
        let record = parse_effective_grant(grant)?;
        if policy.validate_grant(grant, &record)?.is_some() {
            by_scope
                .entry(record.scope)
                .or_insert_with(HashSet::new)
                .extend(record.privileges);
        }
    }
    Ok(by_scope)
}

struct RuntimeGrantPolicy {
    application_scope: String,
    checkpoint_scope: String,
    journal_scope: String,
    conflict_scope: String,
    inventory_scope: String,
}

impl RuntimeGrantPolicy {
    fn new(
        application_schema: &str,
        checkpoint_table: &str,
        journal_table: &str,
        conflict_table: &str,
        inventory_procedure: &str,
    ) -> Self {
        Self {
            application_scope: format!("{application_schema}.*").to_ascii_uppercase(),
            checkpoint_scope: checkpoint_table.to_ascii_uppercase(),
            journal_scope: journal_table.to_ascii_uppercase(),
            conflict_scope: conflict_table.to_ascii_uppercase(),
            inventory_scope: exact_procedure_scope(inventory_procedure),
        }
    }

    fn validate_grant(
        &self,
        grant: &str,
        record: &GrantRecord,
    ) -> Result<Option<RuntimeGrantScope>, String> {
        reject_bypass(grant, record)?;
        if record.scope == "*.*" {
            return validate_global_grant(grant, record);
        }
        let scope = self.scope_policy(&record.scope).ok_or_else(|| {
            format!("CDC runtime grant targets an unapproved control-plane scope: {grant}")
        })?;
        match scope {
            RuntimeGrantScope::Application => validate_application_grant(grant, record),
            RuntimeGrantScope::Control(allowed) => validate_control_grant(grant, record, allowed),
        }
    }

    fn scope_policy(&self, scope: &str) -> Option<RuntimeGrantScope> {
        if scope == self.application_scope {
            return Some(RuntimeGrantScope::Application);
        }
        let privileges = match scope {
            scope if scope == self.checkpoint_scope => CHECKPOINT_PRIVILEGES,
            scope if scope == self.journal_scope => JOURNAL_PRIVILEGES,
            scope if scope == self.conflict_scope => JOURNAL_PRIVILEGES,
            scope if scope == self.inventory_scope => EXECUTE_PRIVILEGES,
            "PROCEDURE CDC.ROW_CONFLICTS_TRIGGER_INVENTORY" => EXECUTE_PRIVILEGES,
            _ => return None,
        };
        Some(RuntimeGrantScope::Control(privileges))
    }
}

fn reject_bypass(grant: &str, record: &GrantRecord) -> Result<(), String> {
    if record.role_or_proxy || record.grant_option {
        Err(format!(
            "CDC runtime grant is a role/proxy/grant-option bypass: {grant}"
        ))
    } else {
        Ok(())
    }
}

fn validate_global_grant(
    grant: &str,
    record: &GrantRecord,
) -> Result<Option<RuntimeGrantScope>, String> {
    if record.privileges == ["USAGE"] {
        Ok(None)
    } else {
        Err(format!(
            "CDC runtime global grant is broader than USAGE: {grant}"
        ))
    }
}

fn validate_application_grant(
    grant: &str,
    record: &GrantRecord,
) -> Result<Option<RuntimeGrantScope>, String> {
    if record.privileges.iter().any(|privilege| {
        !APPLICATION_DML_PRIVILEGES.contains(&privilege.as_str())
            && !APPLICATION_DDL_PRIVILEGES.contains(&privilege.as_str())
    }) {
        Err(format!(
            "CDC runtime application grant contains unsupported privilege: {grant}"
        ))
    } else {
        Ok(Some(RuntimeGrantScope::Application))
    }
}

fn validate_control_grant(
    grant: &str,
    record: &GrantRecord,
    allowed: &'static [&'static str],
) -> Result<Option<RuntimeGrantScope>, String> {
    if record
        .privileges
        .iter()
        .any(|privilege| !allowed.contains(&privilege.as_str()))
    {
        Err(format!(
            "CDC runtime control-plane grant is broader than specified: {grant}"
        ))
    } else {
        Ok(Some(RuntimeGrantScope::Control(allowed)))
    }
}

fn validate_required_runtime_scopes(
    by_scope: &HashMap<String, HashSet<String>>,
    policy: &RuntimeGrantPolicy,
) -> Result<(), String> {
    let required = [
        (&policy.checkpoint_scope, CHECKPOINT_PRIVILEGES),
        (&policy.journal_scope, JOURNAL_PRIVILEGES),
        (&policy.conflict_scope, JOURNAL_PRIVILEGES),
        (&policy.inventory_scope, EXECUTE_PRIVILEGES),
        (&policy.application_scope, APPLICATION_DML_PRIVILEGES),
        (&policy.application_scope, APPLICATION_DDL_PRIVILEGES),
    ];
    for (scope, privileges) in required {
        validate_scope_privileges(by_scope, scope, privileges)?;
    }
    Ok(())
}

fn validate_scope_privileges(
    by_scope: &HashMap<String, HashSet<String>>,
    scope: &str,
    privileges: &[&str],
) -> Result<(), String> {
    let actual = by_scope
        .get(scope)
        .ok_or_else(|| format!("CDC runtime grant missing for {scope}"))?;
    if privileges
        .iter()
        .all(|privilege| actual.contains(*privilege))
    {
        Ok(())
    } else {
        Err(format!(
            "CDC runtime grant missing on {scope}: expected {privileges:?}, found {actual:?}"
        ))
    }
}

fn exact_procedure_scope(inventory_procedure: &str) -> String {
    format!("PROCEDURE {inventory_procedure}").to_ascii_uppercase()
}
