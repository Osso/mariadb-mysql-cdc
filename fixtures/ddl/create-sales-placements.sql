CREATE TABLE IF NOT EXISTS `sales_placements` (
    `id`                 INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `sale_id`            INT UNSIGNED NOT NULL,
    -- Plan target that produced it: app_home_deals, app_next_store, archetype:<slug>, publisher:<key id>, landing...
    `target`             VARCHAR(80) NOT NULL,
    `placement_type`     VARCHAR(40) NOT NULL,
    `target_key_id`      INT UNSIGNED DEFAULT NULL,
    `content_section_id` INT UNSIGNED DEFAULT NULL,
    `custom_card_id`     INT UNSIGNED DEFAULT NULL,
    `comic_id`           INT UNSIGNED DEFAULT NULL,
    -- Values written at build/sync time; drift = live row differs from these.
    `rule_json`          LONGTEXT NOT NULL,
    `is_active`          TINYINT(1) UNSIGNED NOT NULL DEFAULT 1,
    `creator_id`         INT UNSIGNED NOT NULL DEFAULT 0,
    `create_time`        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updater_id`         INT UNSIGNED DEFAULT NULL,
    `update_time`        TIMESTAMP NULL DEFAULT NULL,
    KEY `idx_sp_sale` (`sale_id`, `is_active`),
    KEY `idx_sp_section` (`content_section_id`),
    KEY `idx_sp_card` (`custom_card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
