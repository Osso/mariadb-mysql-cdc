CREATE TABLE IF NOT EXISTS `recsys_rail_experiments` (
  `id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `experiment_key` VARCHAR(100) NOT NULL,
  `content_section_id` INT(11) UNSIGNED NOT NULL,
  `experiment_mode` VARCHAR(20) NOT NULL COMMENT 'rerank | source',
  `recsys_surface` VARCHAR(50) NOT NULL,
  `recsys_params` JSON DEFAULT NULL,
  `candidate_count` SMALLINT UNSIGNED DEFAULT NULL COMMENT 'rerank only; NULL = section count',
  `min_items` SMALLINT UNSIGNED NOT NULL DEFAULT 3,
  `treatment_pct` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `salt` VARCHAR(40) NOT NULL,
  `status` VARCHAR(20) NOT NULL DEFAULT 'draft' COMMENT 'draft | running | stopped',
  `start_time` DATETIME DEFAULT NULL,
  `end_time` DATETIME DEFAULT NULL,
  `create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` TIMESTAMP NULL DEFAULT NULL,
  UNIQUE KEY `uk_experiment_key` (`experiment_key`),
  KEY `idx_section_status` (`content_section_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci
