/* ApplicationName=DBeaver 26.2.0 - SQLEditor <Script.sql> */ -- Named shelves group facet terms.
-- Editors maintain an ordered set of format-specific shelves.
CREATE TABLE IF NOT EXISTS `storefront_chips` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `tab` ENUM('western','manga','webtoon') NOT NULL,
  `chip_key` VARCHAR(64) NOT NULL,
  `display_name` VARCHAR(128) NOT NULL,
  `display_order` SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  `is_active` TINYINT(1) NOT NULL DEFAULT 1,
  `updater_id` INT UNSIGNED DEFAULT NULL,
  `update_time` TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
  `creator_id` INT UNSIGNED DEFAULT NULL,
  `create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_chip_tab_key` (`tab`, `chip_key`),
  KEY `idx_chip_tab_order` (`tab`, `is_active`, `display_order`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
