/* ApplicationName=DBeaver 26.2.0 - SQLEditor <Script.sql> */ CREATE TABLE IF NOT EXISTS `kg_comic_facets` (
    `comic_id`             MEDIUMINT UNSIGNED NOT NULL,
    `facet_type`           VARCHAR(16)  NOT NULL,          -- tone | genre | theme | demographic | ...
    `value`                VARCHAR(64)  NOT NULL,          -- matches kg_release_facets.value
    `release_hits`         SMALLINT UNSIGNED NOT NULL,     -- releases in the generation carrying it
    `release_total`        SMALLINT UNSIGNED NOT NULL,     -- releases in the generation, the denominator
    `release_share`        DECIMAL(4,3) NOT NULL,          -- hits/total; ranking key and confidence proxy
    `facet_rank`           TINYINT UNSIGNED NOT NULL,      -- 1 = primary. `rank` is reserved in MariaDB 10.2+
    `extractor_version`    VARCHAR(64)  NOT NULL,
    `facet_vocab_version`  VARCHAR(80)  NOT NULL,
    `update_time`          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`comic_id`, `facet_type`, `value`),
    -- Serves the storefront read: every title carrying <axis, value>, best-supported first.
    KEY `idx_facet_lookup` (`facet_type`, `value`, `release_share`),
    -- Serves "the primary genre for these titles" without a filesort.
    KEY `idx_comic_axis_rank` (`comic_id`, `facet_type`, `facet_rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
