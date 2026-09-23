CREATE TABLE IF NOT EXISTS `home_feed_contributor_cards` (
    `id`                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    -- Internal label for the admin list. Never on the wire.
    `name`               VARCHAR(255) NOT NULL,
    -- The raw credit string, as comics_contributors stores it. This is the key
    -- /v1/contributors resolves by, so it must match a real credit exactly;
    -- the admin screen picks it rather than accepting free text.
    `contributor_name`   VARCHAR(255) NOT NULL,
    -- What the card renders. Defaults to contributor_name at author time, but
    -- stays editable: a credit reads "o'neil, dennis" more often than it reads
    -- like a name anyone wants to see on a card.
    `display_name`       VARCHAR(255) NOT NULL,
    -- Role label, not an id: the app resolves the card's craft accent by
    -- matching this text, so it must survive as written. Sourced from
    -- comics_contributors_types in the admin dropdown, hence the 64.
    `role`               VARCHAR(64) DEFAULT NULL,
    -- Sent untruncated; the card collapses it behind its own show-more.
    `description`        TEXT DEFAULT NULL,
    `avatar_url`         VARCHAR(1024) DEFAULT NULL,
    -- Ordered list of suggested assistant questions, as JSON, matching
    -- kg_mantles.assistant_prompts rather than inventing a child table for
    -- what is a short list of plain strings.
    `assistant_prompts`  JSON DEFAULT NULL,
    -- The two curated shelves, as ordered id lists: [int], max 12 each.
    -- Stored the way home_feed_mantle_spotlights stores its own rails rather
    -- than as child tables — the lists are short and bounded, the array order
    -- IS the display order, and nothing looks a selection up by comic or
    -- release, only ever by card. Hydrated per request, so a comic renamed or
    -- a release repriced after curation is current without an edit.
    `comic_ids_json`     JSON DEFAULT NULL,
    `release_ids_json`   JSON DEFAULT NULL,
    -- Per-shelf headings. Eyebrow + title, named and sized as the mantle
    -- spotlight rails name and size theirs: the eyebrow is the small accent
    -- line, the title the large one under it. The app renders both through one
    -- section header. A shelf is keyed on its items, never its wording, so a
    -- heading with nothing under it never reaches the wire.
    `comics_eyebrow`     VARCHAR(80)  DEFAULT NULL,
    `comics_title`       VARCHAR(160) DEFAULT NULL,
    `releases_eyebrow`   VARCHAR(80)  DEFAULT NULL,
    `releases_title`     VARCHAR(160) DEFAULT NULL,
    -- Curator-set window (UTC). Live during [start_time, end_time).
    `start_time`         DATETIME NOT NULL,
    `end_time`           DATETIME NOT NULL,
    -- 1 = staged: served to admins only, so a card can be rehearsed in prod.
    `is_admin_only`      TINYINT(1) NOT NULL DEFAULT 0,
    `display_order`      SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    `created_by_user_id` INT UNSIGNED DEFAULT NULL,
    `create_time`        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY `idx_hfcc_window` (`start_time`, `end_time`),
    KEY `idx_hfcc_contributor` (`contributor_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci