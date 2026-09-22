CREATE TABLE IF NOT EXISTS `home_feed_curated_strip_slides` (
    `id`               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `curated_strip_id` BIGINT UNSIGNED NOT NULL,
    `image_url`        VARCHAR(1024) NOT NULL,
    -- Absent means the slide is inert: no navigation, no tap analytics.
    `cta_url`          VARCHAR(1024) DEFAULT NULL,
    `display_order`    SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    `create_time`      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY `idx_hfcss_strip` (`curated_strip_id`, `display_order`),
    CONSTRAINT `fk_hfcss_strip` FOREIGN KEY (`curated_strip_id`)
        REFERENCES `home_feed_curated_strips` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
