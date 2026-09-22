-- Curated strip cards: hand-authored daily-strip-style feed cards from uploaded images.
-- Served via CuratedStripInjector; authored via /v1/admin/feeds/curated-strips.

CREATE TABLE IF NOT EXISTS `home_feed_curated_strips` (
    `id`                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    -- Internal label for the admin list. Never on the wire.
    `name`               VARCHAR(255) NOT NULL,
    -- NOT NULL unlike home_feed_strip_series.title: a curated card exists only
    -- because someone authored it, so there is no sensible default.
    `title`              VARCHAR(80) NOT NULL,
    `subtitle`           VARCHAR(120) DEFAULT NULL,
    `logo_url`           VARCHAR(1024) DEFAULT NULL,
    -- Panel aspect for this card's slides. 0.65 is the daily-strip default.
    `aspect_ratio`       DECIMAL(4,3) NOT NULL DEFAULT 0.650,
    -- Curator-set window (UTC). Live during [start_time, end_time).
    `start_time`         DATETIME NOT NULL,
    `end_time`           DATETIME NOT NULL,
    -- 1 = staged: served to admins only, so a card can be rehearsed in prod.
    `is_admin_only`      TINYINT(1) NOT NULL DEFAULT 0,
    `display_order`      SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    `created_by_user_id` INT UNSIGNED DEFAULT NULL,
    `create_time`        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY `idx_hfcs_window` (`start_time`, `end_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
