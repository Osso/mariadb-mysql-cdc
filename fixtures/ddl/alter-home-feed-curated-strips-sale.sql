/* ApplicationName=DBeaver 26.2.0 - SQLEditor <Script.sql> */
-- Add sale metadata to curated strips
-- Keep existing display order intact
ALTER TABLE home_feed_curated_strips
  ADD COLUMN sale_id INT UNSIGNED DEFAULT NULL AFTER display_order,
  ADD COLUMN sale_title VARCHAR(80) DEFAULT NULL AFTER sale_id,
  ADD COLUMN sale_subtitle VARCHAR(120) DEFAULT NULL AFTER sale_title,
  ADD COLUMN sale_cta_url VARCHAR(1024) DEFAULT NULL AFTER sale_subtitle,
  ADD COLUMN sale_cover_comic_ids_json JSON DEFAULT NULL AFTER sale_cta_url
