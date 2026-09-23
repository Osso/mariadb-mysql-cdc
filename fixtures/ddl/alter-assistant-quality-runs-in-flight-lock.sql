ALTER TABLE `assistant_quality_runs`
    ADD COLUMN `in_flight_lock` tinyint(1) UNSIGNED
        AS (IF(`status` = 'running' AND `is_active` = 1, 1, NULL)) PERSISTENT
        COMMENT 'single-flight slot: 1 while active+running, NULL otherwise',
    ADD UNIQUE KEY `uk_single_in_flight` (`in_flight_lock`)