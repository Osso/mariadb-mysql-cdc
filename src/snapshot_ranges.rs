use crate::snapshot::SnapshotError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRange {
    pub worker: usize,
    pub start_after: Option<Vec<String>>,
    pub end_at: Option<Vec<String>>,
}

pub fn plan_snapshot_ranges(
    boundaries: Vec<Vec<String>>,
    workers: usize,
) -> Result<Vec<SnapshotRange>, SnapshotError> {
    validate_snapshot_range_plan(&boundaries, workers)?;

    let ranges = (0..workers)
        .map(|worker| SnapshotRange {
            worker,
            start_after: range_start_after(&boundaries, worker),
            end_at: boundaries.get(worker).cloned(),
        })
        .collect();
    Ok(ranges)
}

fn validate_snapshot_range_plan(
    boundaries: &[Vec<String>],
    workers: usize,
) -> Result<(), SnapshotError> {
    if workers == 0 {
        return Err(SnapshotError::InvalidTable(
            "snapshot range planning needs at least one worker".to_string(),
        ));
    }
    if boundaries.len() + 1 != workers {
        return Err(SnapshotError::InvalidTable(
            "snapshot range planning needs exactly workers - 1 boundaries".to_string(),
        ));
    }
    if !snapshot_boundaries_are_strictly_ascending(boundaries) {
        return Err(SnapshotError::InvalidTable(
            "snapshot range boundaries must be strictly ascending".to_string(),
        ));
    }
    Ok(())
}

fn snapshot_boundaries_are_strictly_ascending(boundaries: &[Vec<String>]) -> bool {
    boundaries
        .windows(2)
        .all(|pair| primary_key_is_less(&pair[0], &pair[1]))
}

fn primary_key_is_less(left: &[String], right: &[String]) -> bool {
    let ordering = left
        .iter()
        .zip(right)
        .find_map(|(left_value, right_value)| {
            let ordering = match (left_value.parse::<i128>(), right_value.parse::<i128>()) {
                (Ok(left_number), Ok(right_number)) => left_number.cmp(&right_number),
                _ => left_value.cmp(right_value),
            };
            (ordering != std::cmp::Ordering::Equal).then_some(ordering)
        })
        .unwrap_or_else(|| left.len().cmp(&right.len()));
    ordering.is_lt()
}

fn range_start_after(boundaries: &[Vec<String>], worker: usize) -> Option<Vec<String>> {
    worker
        .checked_sub(1)
        .and_then(|boundary_index| boundaries.get(boundary_index).cloned())
}
