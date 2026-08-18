-- One-time transition for a cdc_stream account provisioned before live conflict
-- evidence and legacy table-sync progress were removed from stream startup.
-- Run with target admin credentials while the stream is stopped or immediately
-- before replacing the old stream image.

REVOKE SELECT, INSERT, UPDATE
    ON cdc.row_conflicts
    FROM 'cdc_stream'@'%';

REVOKE EXECUTE
    ON PROCEDURE cdc.row_conflicts_trigger_inventory
    FROM 'cdc_stream'@'%';

REVOKE SELECT, INSERT, UPDATE
    ON cdc.table_sync_runs
    FROM 'cdc_stream'@'%';

-- Preserve the historical tables, procedures, triggers, and resolver access.
-- The live stream must retain only its application, checkpoint, DDL-journal,
-- and DDL-journal inventory-procedure grants.
SHOW GRANTS FOR 'cdc_stream'@'%';
