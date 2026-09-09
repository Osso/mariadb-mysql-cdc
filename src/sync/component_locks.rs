//! Pure, scope-bounded FK lock planning; callers acquire locks before reading source rows.

use crate::inventory::SchemaInventory;
use std::collections::{BTreeMap, BTreeSet};

/// Return the selected portion of the undirected FK component containing `table`.
/// Unselected children do not expand row-mutation scope.
pub fn selected_fk_component(
    inventory: &SchemaInventory,
    selected: &[String],
    table: &str,
) -> Result<Vec<String>, String> {
    let selected: BTreeSet<&str> = selected.iter().map(String::as_str).collect();
    if !selected.contains(table) {
        return Err(format!("table {table} is not selected"));
    }
    let neighbors = build_fk_neighbors(inventory);
    let mut visited = BTreeSet::new();
    let mut pending = vec![table];
    while let Some(current) = pending.pop() {
        if !selected.contains(current) {
            continue;
        }
        if !visited.insert(current) {
            continue;
        }
        if let Some(adjacent) = neighbors.get(current) {
            pending.extend(adjacent.iter().copied());
        }
    }
    Ok(visited.into_iter().map(str::to_owned).collect())
}

fn build_fk_neighbors(inventory: &SchemaInventory) -> BTreeMap<&str, BTreeSet<&str>> {
    let mut neighbors: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for key in &inventory.foreign_keys {
        if key.referenced_schema != inventory.schema {
            continue;
        }
        let child = key.table.as_str();
        let parent = key.referenced_table.as_str();
        neighbors.entry(child).or_default().insert(parent);
        neighbors.entry(parent).or_default().insert(child);
    }
    neighbors
}

/// Build one WRITE lock statement, quoting database and table identifiers separately.
pub fn build_component_lock_sql(
    database: &str,
    component: &[String],
    quote_ident: impl Fn(&str) -> String,
) -> Result<String, String> {
    if component.is_empty() {
        return Err("cannot lock an empty component".into());
    }
    let mut seen = BTreeSet::new();
    for table in component {
        if !seen.insert(table) {
            return Err(format!("duplicate lock table {table}"));
        }
    }
    let database = quote_ident(database);
    let locks: Vec<_> = component
        .iter()
        .map(|table| format!("{database}.{} WRITE", quote_ident(table)))
        .collect();
    Ok(format!("LOCK TABLES {}", locks.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::ForeignKeyInventory;

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn inventory(edges: &[(&str, &str, &str)]) -> SchemaInventory {
        SchemaInventory {
            schema: "catalog".into(),
            tables: vec![],
            indexes: vec![],
            foreign_keys: edges
                .iter()
                .map(|(child, schema, parent)| ForeignKeyInventory {
                    table: (*child).into(),
                    name: format!("{child}_{parent}"),
                    columns: names(&["parent_id"]),
                    referenced_schema: (*schema).into(),
                    referenced_table: (*parent).into(),
                    referenced_columns: names(&["id"]),
                })
                .collect(),
            views: vec![],
            triggers: vec![],
            routines: vec![],
            events: vec![],
        }
    }

    fn quote_ident(identifier: &str) -> String {
        format!("`{}`", identifier.replace('`', "``"))
    }

    #[test]
    fn follows_transitive_edges_in_both_directions_and_sorts() {
        let mut graph = inventory(&[
            ("pages", "catalog", "books"),
            ("books", "catalog", "publishers"),
            ("reviews", "catalog", "books"),
            ("sessions", "catalog", "users"),
        ]);
        let selected = names(&[
            "users",
            "reviews",
            "publishers",
            "pages",
            "books",
            "sessions",
        ]);
        let expected = names(&["books", "pages", "publishers", "reviews"]);
        for table in ["pages", "publishers", "books"] {
            assert_eq!(
                selected_fk_component(&graph, &selected, table),
                Ok(expected.clone())
            );
        }
        graph.foreign_keys.reverse();
        assert_eq!(
            selected_fk_component(&graph, &selected, "reviews"),
            Ok(expected)
        );
        assert_eq!(
            selected_fk_component(&graph, &selected, "users"),
            Ok(names(&["sessions", "users"]))
        );
    }

    #[test]
    fn cycles_and_self_edges_terminate() {
        let graph = inventory(&[
            ("a", "catalog", "b"),
            ("b", "catalog", "c"),
            ("c", "catalog", "a"),
            ("a", "catalog", "a"),
        ]);
        assert_eq!(
            selected_fk_component(&graph, &names(&["c", "b", "a"]), "a"),
            Ok(names(&["a", "b", "c"]))
        );
    }

    #[test]
    fn isolated_table_and_other_schema_edges_do_not_expand_component() {
        let graph = inventory(&[("books", "archive", "missing")]);
        assert_eq!(
            selected_fk_component(&graph, &names(&["books", "isolated"]), "books"),
            Ok(names(&["books"]))
        );
        assert_eq!(
            selected_fk_component(&graph, &names(&["isolated"]), "isolated"),
            Ok(names(&["isolated"]))
        );
    }

    #[test]
    fn rejects_empty_selection_and_unselected_start() {
        let graph = inventory(&[]);
        for selected in [vec![], names(&["books"])] {
            assert_eq!(
                selected_fk_component(&graph, &selected, "missing"),
                Err("table missing is not selected".into())
            );
        }
    }

    #[test]
    fn does_not_expand_component_into_unselected_tables() {
        for edges in [
            vec![("a", "catalog", "b"), ("b", "catalog", "outside")],
            vec![("a", "catalog", "b"), ("outside", "catalog", "b")],
        ] {
            assert_eq!(
                selected_fk_component(&inventory(&edges), &names(&["a", "b"]), "a"),
                Ok(names(&["a", "b"]))
            );
        }
    }

    #[test]
    fn unrelated_unselected_edges_do_not_block_component() {
        let graph = inventory(&[("outside", "catalog", "elsewhere")]);
        assert_eq!(
            selected_fk_component(&graph, &names(&["books"]), "books"),
            Ok(names(&["books"]))
        );
    }

    #[test]
    fn builds_entire_component_with_quoted_identifiers() {
        assert_eq!(
            build_component_lock_sql(
                "cat`alog",
                &names(&["book.pages", "pub`lishers"]),
                quote_ident
            ),
            Ok(
                "LOCK TABLES `cat``alog`.`book.pages` WRITE, `cat``alog`.`pub``lishers` WRITE"
                    .into()
            )
        );
        assert_eq!(
            build_component_lock_sql("db", &names(&["books"]), |name| format!("[{name}]")),
            Ok("LOCK TABLES [db].[books] WRITE".into())
        );
    }

    #[test]
    fn rejects_empty_and_duplicate_lock_components() {
        assert_eq!(
            build_component_lock_sql("db", &[], quote_ident),
            Err("cannot lock an empty component".into())
        );
        assert_eq!(
            build_component_lock_sql("db", &names(&["books", "pages", "books"]), quote_ident),
            Err("duplicate lock table books".into())
        );
    }
}
