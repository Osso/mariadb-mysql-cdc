use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

const DEFAULT_SYNC_PROGRESS_CACHE_TIMEOUT: Duration = Duration::from_millis(1500);

pub(super) struct CachedSyncProgress {
    pub(super) report: String,
    pub(super) modified: SystemTime,
}

pub(super) fn write_sync_progress_cache(key: &str, report: &str) {
    let Some(path) = sync_progress_cache_path(key) else {
        return;
    };
    if let Some(parent) = path.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        eprintln!(
            "sync_progress_cache_write_failed path={} error={error}",
            path.display()
        );
        return;
    }
    if let Err(error) = fs::write(&path, report) {
        eprintln!(
            "sync_progress_cache_write_failed path={} error={error}",
            path.display()
        );
    }
}

pub(super) fn read_sync_progress_cache(key: &str) -> Option<CachedSyncProgress> {
    let path = sync_progress_cache_path(key)?;
    let report = fs::read_to_string(&path).ok()?;
    let modified = fs::metadata(&path).ok()?.modified().ok()?;
    Some(CachedSyncProgress { report, modified })
}

pub(super) fn format_cached_sync_progress(cache: &CachedSyncProgress, reason: &str) -> String {
    let age_seconds = SystemTime::now()
        .duration_since(cache.modified)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    format!(
        "sync_progress_cache status=stale age_seconds={age_seconds} reason={}\n{}",
        status_token(reason),
        cache.report
    )
}

pub(super) fn sync_progress_cache_timeout() -> Duration {
    std::env::var("MARIADB_MYSQL_CDC_SYNC_PROGRESS_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_SYNC_PROGRESS_CACHE_TIMEOUT)
}

fn sync_progress_cache_path(key: &str) -> Option<PathBuf> {
    let file_name = format!("sync-progress-{}.txt", safe_cache_key(key));
    std::env::var_os("HOME").map(|home| {
        PathBuf::from(home)
            .join(".cache")
            .join("mariadb-mysql-cdc")
            .join(file_name)
    })
}

fn safe_cache_key(key: &str) -> String {
    key.chars()
        .map(|character| match character {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' => character,
            _ => '_',
        })
        .collect()
}

fn status_token(value: &str) -> String {
    value.replace(char::is_whitespace, "_")
}
