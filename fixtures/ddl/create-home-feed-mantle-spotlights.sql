CREATE TABLE IF NOT EXISTS `home_feed_mantle_spotlights` (
    `id`                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    -- Seed source. mantle_key is a frozen copy: kg_mantles.mantle_key can be re-authored, and the card
    -- must keep pointing at the identity the curator picked.
    `mantle_id`             BIGINT UNSIGNED NOT NULL,
    `mantle_key`            VARCHAR(255) NOT NULL,
    `artist_id`             MEDIUMINT UNSIGNED NOT NULL,
    -- Curator-set active window (UTC). The card is live during [start_time, end_time).
    -- Overlaps/gaps are the curator's responsibility — no rotation, no unique key (as with RIGI).
    `start_time`            DATETIME NOT NULL,
    `end_time`              DATETIME NOT NULL,
    -- 1 = staged: served to admins only, so a card can be rehearsed in production.
    `is_admin_only`         TINYINT(1) NOT NULL DEFAULT 0,
    -- Ordering when several cards are live at once (then start_time, then id).
    `display_order`         SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    -- The card's tap target. Required: a spotlight with nowhere to go is not worth serving.
    `cta_url`               VARCHAR(1024) NOT NULL,
    `cta_label`             VARCHAR(80) DEFAULT NULL,
    -- Sale block. Emitted only when title, description and banner_url are ALL set; the logo and
    -- the trailing promo art are decoration. Otherwise it serves as a plain spotlight, sale: null.
    `sale_title`            VARCHAR(255) DEFAULT NULL,
    `sale_description`      VARCHAR(1000) DEFAULT NULL,
    `sale_banner_url`       VARCHAR(1024) DEFAULT NULL,
    `sale_logo_url`         VARCHAR(1024) DEFAULT NULL,
    -- Trailing art on the sale banner, NOT a link: the mobile client renders promo_url as an image
    -- and `cta_url` is already the card's only tap target. Optional.
    `sale_promo_url`        VARCHAR(1024) DEFAULT NULL,
    -- Optional second destination: when set, the banner is its own tap target and the rest of the
    -- card still opens `cta_url`. NULL means the banner shares the card's destination, which is the
    -- app's default and NOT a broken state.
    `sale_cta_url`          VARCHAR(1024) DEFAULT NULL,
    -- Curated priced-releases rail ("titles in this sale"). Ids only; each row is hydrated per
    -- request so it carries price and ownership, which is the whole reason the card has this rail
    -- instead of reusing the hub's unpriced series list.
    `sale_release_ids_json` JSON DEFAULT NULL, -- [int], ordered, max 12
    -- Per-campaign section wording. Server-driven because "Series & Sagas" is wrong copy for a row
    -- of priced sale issues. NULL leaves the client on the mantle page's own wording.
    `releases_eyebrow`      VARCHAR(80) DEFAULT NULL,
    `releases_title`        VARCHAR(120) DEFAULT NULL,
    `series_eyebrow`        VARCHAR(80) DEFAULT NULL,
    `series_title`          VARCHAR(120) DEFAULT NULL,
    `start_here_eyebrow`    VARCHAR(80) DEFAULT NULL,
    `start_here_title`      VARCHAR(120) DEFAULT NULL,
    -- 0 drops the per-pick blurbs and shrinks that rail; 1 (default) keeps the mantle behaviour.
    `start_here_show_description` TINYINT(1) NOT NULL DEFAULT 1,
    -- Editable character copy, seeded from the kg_mantles overlay and free to diverge from it.
    -- Column names mirror kg_mantles so the seed is a straight copy.
    `name`                  VARCHAR(255) NOT NULL,
    `kind_label`            VARCHAR(255) DEFAULT NULL,
    `logline`               TEXT DEFAULT NULL,
    `alignment`             VARCHAR(16) DEFAULT NULL,
    -- Processed CDN URL (not an asset id), exactly as kg_mantles.hero_art_asset stores it.
    `hero_art_asset`        VARCHAR(1024) DEFAULT NULL,
    `theme_color`           VARCHAR(16) DEFAULT NULL,
    `theme_text_color`      VARCHAR(16) DEFAULT NULL,
    `theme_bg_color`        VARCHAR(16) DEFAULT NULL,
    `assistant_prompts`     JSON DEFAULT NULL, -- string[]
    -- Curated lists as IDS ONLY: both hydrate per request (pricing, ownership, read progress are
    -- viewer-derived and must never be frozen into a shared snapshot).
    `start_here_json`       JSON DEFAULT NULL, -- [{"release_id":int,"blurb":?string}], ordered, max 3
    `best_run_comic_ids_json` JSON DEFAULT NULL, -- [int], ordered, max 12
    -- The viewer-INDEPENDENT hub blocks, frozen at seed/reseed time so a sale card cannot shift
    -- under the curator: {"vitals":{...},"bearers":[...],"connections":[...]}.
    `hub_snapshot_json`     JSON DEFAULT NULL,
    `created_by`            INT UNSIGNED DEFAULT NULL,
    `create_time`           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY `idx_hfms_window` (`start_time`, `end_time`),
    KEY `idx_hfms_mantle` (`mantle_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
