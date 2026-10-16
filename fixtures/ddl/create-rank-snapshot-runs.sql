/* ApplicationName=DBeaver 26.2.0 - SQLEditor <Script.sql> */ -- One row per completed snapshot run, so an empty chart still records that it ran that day.
CREATE TABLE IF NOT EXISTS `rank_snapshot_runs` (
  `scope` enum('comics','releases') NOT NULL,
  `statistic` varchar(16) NOT NULL,
  `period` smallint(5) unsigned NOT NULL,
  `snapshot_date` date NOT NULL,
  `item_count` mediumint(8) unsigned NOT NULL DEFAULT 0,
  `create_time` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`scope`,`statistic`,`period`,`snapshot_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
