//! Pure scheduling for selected tables. Each batch must finish before the next starts.

use crate::inventory::ForeignKeyInventory;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    ParentFirst,
    ChildFirst,
}

#[derive(Debug, Eq, PartialEq)]
pub enum DependencyOrderError {
    ZeroParallelism,
    DuplicateTable(String),
    SelfDependency(String),
    Cycle,
}

pub fn plan_dependency_batches(
    selected: &[String],
    foreign_keys: &[ForeignKeyInventory],
    schema: &str,
    direction: Direction,
    max_parallelism: usize,
) -> Result<Vec<Vec<String>>, DependencyOrderError> {
    if max_parallelism == 0 {
        return Err(DependencyOrderError::ZeroParallelism);
    }
    let mut successors = BTreeMap::new();
    for table in selected {
        if successors.insert(table.as_str(), BTreeSet::new()).is_some() {
            return Err(DependencyOrderError::DuplicateTable(table.clone()));
        }
    }
    add_dependencies(&mut successors, foreign_keys, schema, direction)?;
    schedule_batches(&successors, max_parallelism)
}

fn add_dependencies<'a>(
    successors: &mut BTreeMap<&'a str, BTreeSet<&'a str>>,
    foreign_keys: &'a [ForeignKeyInventory],
    schema: &str,
    direction: Direction,
) -> Result<(), DependencyOrderError> {
    for key in foreign_keys {
        if key.referenced_schema != schema {
            continue;
        }
        let child = key.table.as_str();
        let parent = key.referenced_table.as_str();
        if !successors.contains_key(child) || !successors.contains_key(parent) {
            continue;
        }
        if child == parent {
            return Err(DependencyOrderError::SelfDependency(child.to_owned()));
        }
        let (before, after) = match direction {
            Direction::ParentFirst => (parent, child),
            Direction::ChildFirst => (child, parent),
        };
        successors
            .get_mut(before)
            .expect("selected table")
            .insert(after);
    }
    Ok(())
}

fn count_dependencies<'a>(
    successors: &BTreeMap<&'a str, BTreeSet<&'a str>>,
) -> BTreeMap<&'a str, usize> {
    let mut counts: BTreeMap<_, _> = successors.keys().map(|&table| (table, 0)).collect();
    for &after in successors.values().flatten() {
        *counts.get_mut(after).expect("selected successor") += 1;
    }
    counts
}

fn schedule_batches(
    successors: &BTreeMap<&str, BTreeSet<&str>>,
    max_parallelism: usize,
) -> Result<Vec<Vec<String>>, DependencyOrderError> {
    let mut counts = count_dependencies(successors);
    let mut ready: BTreeSet<_> = counts
        .iter()
        .filter_map(|(&table, &count)| (count == 0).then_some(table))
        .collect();
    let mut batches = Vec::new();
    let mut scheduled = 0;
    while !ready.is_empty() {
        let batch: Vec<_> = ready.iter().copied().take(max_parallelism).collect();
        for table in &batch {
            ready.remove(table);
        }
        for table in &batch {
            for after in &successors[table] {
                let count = counts.get_mut(after).expect("selected successor");
                *count -= 1;
                if *count == 0 {
                    ready.insert(*after);
                }
            }
        }
        scheduled += batch.len();
        batches.push(batch.into_iter().map(str::to_owned).collect());
    }
    if scheduled != successors.len() {
        return Err(DependencyOrderError::Cycle);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn fk(child: &str, parent: &str) -> ForeignKeyInventory {
        ForeignKeyInventory {
            table: child.into(),
            name: format!("{child}_{parent}_fk"),
            columns: names(&["parent_id"]),
            referenced_schema: "catalog".into(),
            referenced_table: parent.into(),
            referenced_columns: names(&["id"]),
        }
    }

    fn plan(
        selected: &[&str],
        keys: &[ForeignKeyInventory],
        direction: Direction,
        limit: usize,
    ) -> Result<Vec<Vec<String>>, DependencyOrderError> {
        plan_dependency_batches(&names(selected), keys, "catalog", direction, limit)
    }

    #[test]
    fn parents_finish_before_children_and_grandchildren() {
        let keys = [fk("releases", "comics"), fk("pages", "releases")];
        assert_eq!(
            plan(
                &["pages", "releases", "comics"],
                &keys,
                Direction::ParentFirst,
                8
            ),
            Ok(vec![
                names(&["comics"]),
                names(&["releases"]),
                names(&["pages"])
            ])
        );
    }

    #[test]
    fn children_finish_before_parents() {
        let keys = [fk("releases", "comics"), fk("pages", "releases")];
        assert_eq!(
            plan(
                &["comics", "pages", "releases"],
                &keys,
                Direction::ChildFirst,
                8
            ),
            Ok(vec![
                names(&["pages"]),
                names(&["releases"]),
                names(&["comics"])
            ])
        );
    }

    #[test]
    fn diamond_waits_for_both_parents_and_ignores_duplicate_edges() {
        let keys = [
            fk("left", "root"),
            fk("right", "root"),
            fk("leaf", "left"),
            fk("leaf", "right"),
            fk("leaf", "right"),
        ];
        assert_eq!(
            plan(
                &["right", "leaf", "root", "left"],
                &keys,
                Direction::ParentFirst,
                2
            ),
            Ok(vec![
                names(&["root"]),
                names(&["left", "right"]),
                names(&["leaf"])
            ])
        );
    }

    #[test]
    fn independent_tables_are_sorted_bounded_and_preserved() {
        for direction in [Direction::ParentFirst, Direction::ChildFirst] {
            assert_eq!(
                plan(&["z", "b", "a", "c", "X"], &[], direction, 2),
                Ok(vec![names(&["X", "a"]), names(&["b", "c"]), names(&["z"])])
            );
            assert_eq!(
                plan(&["b", "a"], &[], direction, 1),
                Ok(vec![names(&["a"]), names(&["b"])])
            );
            assert_eq!(plan(&[], &[], direction, 1), Ok(vec![]));
        }
    }

    #[test]
    fn selection_and_inventory_order_do_not_change_batches() {
        let keys = [fk("releases", "comics"), fk("pages", "releases")];
        for direction in [Direction::ParentFirst, Direction::ChildFirst] {
            assert_eq!(
                plan(
                    &["pages", "comics", "releases", "artists"],
                    &keys,
                    direction,
                    2
                ),
                plan(
                    &["artists", "releases", "comics", "pages"],
                    &[keys[1].clone(), keys[0].clone()],
                    direction,
                    2
                )
            );
        }
    }

    #[test]
    fn external_and_unselected_tables_impose_no_edge() {
        let mut external = fk("comics", "artists");
        external.referenced_schema = "external".into();
        let keys = [
            external,
            fk("artists", "missing"),
            fk("unselected", "unselected"),
        ];
        assert_eq!(
            plan(&["comics", "artists"], &keys, Direction::ParentFirst, 2),
            Ok(vec![names(&["artists", "comics"])])
        );
    }

    #[test]
    fn rejects_cycles_even_with_independent_work() {
        let keys = [
            fk("comics", "releases"),
            fk("releases", "pages"),
            fk("pages", "comics"),
        ];
        for direction in [Direction::ParentFirst, Direction::ChildFirst] {
            assert_eq!(
                plan(
                    &["comics", "releases", "pages", "artists"],
                    &keys,
                    direction,
                    1
                ),
                Err(DependencyOrderError::Cycle)
            );
        }
    }

    #[test]
    fn rejects_self_dependencies_duplicates_and_zero_parallelism() {
        assert_eq!(
            plan(
                &["comics"],
                &[fk("comics", "comics")],
                Direction::ParentFirst,
                1
            ),
            Err(DependencyOrderError::SelfDependency("comics".into()))
        );
        assert_eq!(
            plan(&["comics", "comics"], &[], Direction::ParentFirst, 1),
            Err(DependencyOrderError::DuplicateTable("comics".into()))
        );
        assert_eq!(
            plan(&[], &[], Direction::ParentFirst, 0),
            Err(DependencyOrderError::ZeroParallelism)
        );
    }
}
