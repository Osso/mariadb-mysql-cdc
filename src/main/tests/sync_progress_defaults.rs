use crate::live::ApplyBinlogConfig;
use crate::{recovery_config_from_apply, resync_config_from_apply};

#[test]
fn sync_progress_defaults_use_the_unified_table_for_recovery_callers() {
    let apply = ApplyBinlogConfig::default();
    let recovery = recovery_config_from_apply(
        apply.clone(),
        "authorization.json".to_string(),
        "source-db".to_string(),
    );
    let resync = resync_config_from_apply(apply, "source-db".to_string(), 4);

    assert_eq!(recovery.progress_table, "cdc.sync_runs");
    assert_eq!(resync.progress_table, "cdc.sync_runs");
}
