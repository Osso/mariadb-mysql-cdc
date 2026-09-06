ALTER TABLE `releases`
  DROP INDEX `idx_downloads_sort`,
  ADD INDEX `idx_downloads_sort` (`is_deleted`, `is_published`, `is_visible`, `comic_is_visible`, `lang_id`, `published_time` DESC, `comic_id` ASC, `id` ASC),
  ALGORITHM=INPLACE, LOCK=NONE