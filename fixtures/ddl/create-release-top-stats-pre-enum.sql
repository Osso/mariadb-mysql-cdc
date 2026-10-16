CREATE TABLE `releases_top_stats` (
  `release_id` int(11) unsigned NOT NULL,
  `statistic` enum('views','popularity','likes','purchases') NOT NULL,
  `value_1_day` float unsigned NOT NULL DEFAULT 0,
  `value_7_days` float unsigned NOT NULL DEFAULT 0,
  `value_30_days` float unsigned NOT NULL DEFAULT 0,
  `value_90_days` float unsigned NOT NULL DEFAULT 0,
  `value_365_days` float unsigned NOT NULL DEFAULT 0,
  `update_time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`release_id`,`statistic`),
  KEY `idx_release_7_30_90` (`value_7_days`,`value_30_days`,`value_90_days`),
  KEY `idx_release_30_90_365` (`value_30_days`,`value_90_days`,`value_365_days`),
  KEY `idx_release_90_365` (`value_90_days`,`value_365_days`),
  KEY `idx_release_365` (`value_365_days`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
