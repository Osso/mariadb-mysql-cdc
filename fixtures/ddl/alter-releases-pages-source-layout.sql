ALTER TABLE `releases_pages`
  ADD COLUMN IF NOT EXISTS `source_layout` JSON DEFAULT NULL AFTER `comic_asset_id`
