-- Phase 1: exact tool-to-turn joins. Existing rows remain unattributed (NULL).
-- Apply before deploying the recorder changes. No backfill from timestamps.
ALTER TABLE `llm_audit_log`
    ADD COLUMN IF NOT EXISTS `turn_uuid` CHAR(36) DEFAULT NULL COMMENT 'Exact assistant turn; NULL for historical or Capy rows' AFTER `conversation_uuid`,
    ADD COLUMN IF NOT EXISTS `step` SMALLINT UNSIGNED DEFAULT NULL COMMENT 'Zero-based assistant model loop step' AFTER `turn_uuid`,
    MODIFY COLUMN `tool_use_id` VARCHAR(64) DEFAULT NULL COMMENT 'Provider tool call ID (Capy or assistant)',
    ADD INDEX IF NOT EXISTS `idx_turn_step` (`turn_uuid`, `step`), ALGORITHM=INPLACE, LOCK=NONE
