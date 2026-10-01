ALTER TABLE `assistant_quality_verdicts`
    ADD COLUMN IF NOT EXISTS `user_id` int(12) UNSIGNED DEFAULT NULL AFTER `conversation_uuid`,
    ADD COLUMN IF NOT EXISTS `conversation_start` datetime DEFAULT NULL COMMENT 'llm_conversations.create_time, UTC' AFTER `user_id`,
    ADD COLUMN IF NOT EXISTS `account_age_bucket` varchar(24) DEFAULT NULL COMMENT 'new_0_7d|new_7_30d|established_30d_plus|unknown' AFTER `conversation_start`,
    ADD COLUMN IF NOT EXISTS `gold_status` varchar(12) DEFAULT NULL COMMENT 'gold_paid|gold_trial|gold_grant|free|unknown' AFTER `account_age_bucket`,
    ADD COLUMN IF NOT EXISTS `sample_kind` varchar(12) NOT NULL DEFAULT 'random' COMMENT 'random|experiment' AFTER `gold_status`,
    ADD COLUMN IF NOT EXISTS `experiment_key` varchar(64) DEFAULT NULL COMMENT 'NULL for the random sample' AFTER `sample_kind`,
    ADD COLUMN IF NOT EXISTS `variant` varchar(32) DEFAULT NULL COMMENT 'NULL for the random sample' AFTER `experiment_key`,
    ADD COLUMN IF NOT EXISTS `rubric_version` int(11) UNSIGNED DEFAULT NULL COMMENT 'llm_prompts.id of the rubric that judged it' AFTER `variant`,
    ADD COLUMN IF NOT EXISTS `sample_slot` varchar(64) AS (COALESCE(`experiment_key`, '')) PERSISTENT
        COMMENT 'per-run uniqueness slot: empty for the random sample, else the experiment key' AFTER `rubric_version`,
    ADD UNIQUE KEY IF NOT EXISTS `uk_run_slot_conversation` (`run_id`, `sample_slot`, `conversation_id`),
    ADD UNIQUE KEY IF NOT EXISTS `uk_experiment_conversation` (`experiment_key`, `conversation_id`),
    DROP KEY IF EXISTS `uk_run_conversation`,
    ADD CONSTRAINT IF NOT EXISTS `chk_aqv_sample_kind_experiment_key`
        CHECK ((`sample_kind` IN ('random', 'experiment')) AND ((`sample_kind` = 'random') = (`experiment_key` IS NULL)))