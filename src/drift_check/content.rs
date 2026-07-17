use super::query::{
    open_connection, query_checksum_columns, query_chunk_checksum, query_primary_key,
    query_primary_key_endpoints, query_primary_key_endpoints_in_range, source_query_config,
    target_query_config,
};
use super::{
    ContentDriftRange, ContentDriftSummary, DriftCheckConfig, DriftCheckError, DriftCheckObserver,
    MAX_MISMATCH_RANGES, MIN_REPAIR_RANGE_ROWS,
};
use crate::checksum::ChecksumColumn;
use mysql::Conn;
use std::cell::RefCell;

pub(crate) fn compare_table_content(
    config: &DriftCheckConfig,
    table: &str,
    observer: &impl DriftCheckObserver,
) -> Result<ContentDriftSummary, DriftCheckError> {
    match ChecksumCompareContext::load(config, table)? {
        ContentCheckPlan::Compare(context) => compare_checksum_chunks(&context, observer),
        ContentCheckPlan::Skip(reason) => {
            observer.content_skipped(table, &reason);
            Ok(ContentDriftSummary {
                skipped_reason: Some(reason),
                ..ContentDriftSummary::default()
            })
        }
    }
}

enum ContentCheckPlan {
    Compare(ChecksumCompareContext),
    Skip(String),
}

pub(crate) struct ChecksumCompareContext {
    source_conn: RefCell<Conn>,
    target_conn: RefCell<Conn>,
    table: String,
    primary_key: Vec<String>,
    columns: Vec<ChecksumColumn>,
    skipped_columns: Vec<String>,
    chunk_size: usize,
}

impl ChecksumCompareContext {
    fn load(config: &DriftCheckConfig, table: &str) -> Result<ContentCheckPlan, DriftCheckError> {
        let mut source_conn = open_connection(&source_query_config(&config.source))?;
        let primary_key = query_primary_key(&mut source_conn, table)?;
        if primary_key.is_empty() {
            return Ok(ContentCheckPlan::Skip("no primary key".to_string()));
        }
        let all_columns = query_checksum_columns(&mut source_conn, table)?;
        let (columns, skipped_columns) = partition_checksum_columns(all_columns);
        if let Some(unsupported_key) = primary_key.iter().find(|key| skipped_columns.contains(key))
        {
            return Ok(ContentCheckPlan::Skip(format!(
                "primary key column `{unsupported_key}` has an unsupported checksum type"
            )));
        }
        let target_conn = open_connection(&target_query_config(&config.target))?;
        Ok(ContentCheckPlan::Compare(Self {
            source_conn: RefCell::new(source_conn),
            target_conn: RefCell::new(target_conn),
            table: table.to_string(),
            primary_key,
            columns,
            skipped_columns,
            chunk_size: config.chunk_size,
        }))
    }
}

pub(crate) fn partition_checksum_columns(
    columns: Vec<ChecksumColumn>,
) -> (Vec<ChecksumColumn>, Vec<String>) {
    let (supported, skipped): (Vec<_>, Vec<_>) = columns
        .into_iter()
        .partition(|column| crate::checksum::is_supported_checksum_type(&column.data_type));
    let skipped_names = skipped.into_iter().map(|column| column.name).collect();
    (supported, skipped_names)
}

fn compare_checksum_chunks(
    context: &ChecksumCompareContext,
    observer: &impl DriftCheckObserver,
) -> Result<ContentDriftSummary, DriftCheckError> {
    let mut summary = ContentDriftSummary {
        skipped_columns: context.skipped_columns.clone(),
        ..ContentDriftSummary::default()
    };
    let mut start_after = None;

    loop {
        let endpoints = query_primary_key_endpoints(
            &context.source_conn,
            &context.table,
            &context.primary_key,
            start_after.clone(),
            context.chunk_size,
        )?;
        let end_at = endpoints.last().cloned();
        record_checksum_comparison(
            &mut summary,
            context,
            start_after.clone(),
            end_at.clone(),
            observer,
        )?;

        if endpoints.len() < context.chunk_size {
            record_target_tail_checksum(&mut summary, context, end_at, observer)?;
            return Ok(summary);
        }
        start_after = end_at;
    }
}

fn record_checksum_comparison(
    summary: &mut ContentDriftSummary,
    context: &ChecksumCompareContext,
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    observer: &impl DriftCheckObserver,
) -> Result<(), DriftCheckError> {
    observer.content_chunk_started(&context.table, start_after.as_ref(), end_at.as_ref());
    let comparison = compare_checksum_range(context, start_after, end_at)?;
    observer.content_chunk_completed(
        &context.table,
        comparison.source_count(),
        comparison.target_count(),
        comparison.is_mismatch(),
    );
    summary.chunks += 1;
    if comparison.is_mismatch() {
        summary.mismatched_chunks += 1;
        split_or_record_mismatch(summary, context, comparison, observer)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChecksumRangeComparison {
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    source: (u64, u64),
    target: (u64, u64),
}

impl ChecksumRangeComparison {
    fn is_mismatch(&self) -> bool {
        self.source != self.target
    }

    fn source_count(&self) -> u64 {
        self.source.0
    }

    fn target_count(&self) -> u64 {
        self.target.0
    }

    fn drift_range(&self) -> ContentDriftRange {
        ContentDriftRange {
            start_after: self.start_after.clone(),
            end_at: self.end_at.clone(),
            source_count: self.source_count(),
            target_count: self.target_count(),
        }
    }
}

fn compare_checksum_range(
    context: &ChecksumCompareContext,
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
) -> Result<ChecksumRangeComparison, DriftCheckError> {
    let source = checksum_for_range(
        context,
        &context.source_conn,
        start_after.clone(),
        end_at.clone(),
    )?;
    let target = checksum_for_range(
        context,
        &context.target_conn,
        start_after.clone(),
        end_at.clone(),
    )?;
    Ok(ChecksumRangeComparison {
        start_after,
        end_at,
        source,
        target,
    })
}

fn split_or_record_mismatch(
    summary: &mut ContentDriftSummary,
    context: &ChecksumCompareContext,
    comparison: ChecksumRangeComparison,
    observer: &impl DriftCheckObserver,
) -> Result<(), DriftCheckError> {
    let Some(midpoint) = mismatch_midpoint(summary, context, &comparison)? else {
        record_mismatched_range(summary, comparison.drift_range());
        return Ok(());
    };

    record_checksum_comparison(
        summary,
        context,
        comparison.start_after.clone(),
        Some(midpoint.clone()),
        observer,
    )?;
    record_checksum_comparison(
        summary,
        context,
        Some(midpoint),
        comparison.end_at,
        observer,
    )?;
    Ok(())
}

fn record_mismatched_range(summary: &mut ContentDriftSummary, range: ContentDriftRange) {
    if summary.mismatched_ranges.len() >= MAX_MISMATCH_RANGES {
        summary.range_limit_exceeded = true;
    } else {
        summary.mismatched_ranges.push(range);
    }
}

fn mismatch_midpoint(
    summary: &ContentDriftSummary,
    context: &ChecksumCompareContext,
    comparison: &ChecksumRangeComparison,
) -> Result<Option<Vec<String>>, DriftCheckError> {
    if summary.range_limit_exceeded
        || summary.mismatched_ranges.len() >= MAX_MISMATCH_RANGES
        || comparison.source_count() <= MIN_REPAIR_RANGE_ROWS
    {
        return Ok(None);
    }
    let split_size = (comparison.source_count() / 2) as usize;
    let endpoints = query_primary_key_endpoints_in_range(
        &context.source_conn,
        &context.table,
        &context.primary_key,
        comparison.start_after.clone(),
        comparison.end_at.clone(),
        split_size.max(1),
    )?;
    let midpoint = endpoints.last().cloned();
    Ok(midpoint.filter(|value| Some(value.clone()) != comparison.end_at))
}

fn record_target_tail_checksum(
    summary: &mut ContentDriftSummary,
    context: &ChecksumCompareContext,
    end_at: Option<Vec<String>>,
    observer: &impl DriftCheckObserver,
) -> Result<(), DriftCheckError> {
    if end_at.is_some() {
        record_checksum_comparison(summary, context, end_at, None, observer)?;
    }
    Ok(())
}

fn checksum_for_range(
    context: &ChecksumCompareContext,
    conn: &RefCell<Conn>,
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
) -> Result<(u64, u64), DriftCheckError> {
    query_chunk_checksum(
        conn,
        &context.table,
        &context.primary_key,
        &context.columns,
        start_after,
        end_at,
    )
}
