CREATE DEFINER=`root`@`10.%` PROCEDURE `apply_release_move_purchase_repair`()
BEGIN
    DECLARE manifest_rows INT DEFAULT 0;
    DECLARE invalid_rows INT DEFAULT 0;
    DECLARE updated_rows INT DEFAULT 0;
    DECLARE locked_source_rows INT DEFAULT 0;
    DECLARE locked_destination_rows INT DEFAULT 0;
    DECLARE locked_related_rows INT DEFAULT 0;
    DECLARE locked_income_rows INT DEFAULT 0;
    DECLARE expected_income_rows INT DEFAULT 0;
    DECLARE live_candidate_rows INT DEFAULT 0;
    DECLARE unmanifested_rows INT DEFAULT 0;
    DECLARE transaction_isolation VARCHAR(64);

    SELECT @@tx_isolation INTO transaction_isolation;
    IF UPPER(REPLACE(transaction_isolation, ' ', '-'))<>'REPEATABLE-READ' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Repair requires REPEATABLE READ transaction isolation';
    END IF;

    SELECT COUNT(*) INTO manifest_rows FROM release_move_purchase_manifest;
    IF manifest_rows<>93 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Manifest row count changed; expected 93';
    END IF;


    SELECT COUNT(rup_source.id) INTO locked_source_rows
    FROM (SELECT DISTINCT from_release_id FROM release_move_purchase_manifest) manifest_sources
    JOIN releases_users_purchases rup_source FORCE INDEX (idx_release_active)
      ON rup_source.release_id=manifest_sources.from_release_id
     AND rup_source.is_active=1
    FOR UPDATE;
    IF locked_source_rows<>93 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Active source entitlement count changed; expected 93';
    END IF;

    SELECT COUNT(rup_destination.id) INTO locked_destination_rows
    FROM (SELECT DISTINCT to_release_id FROM release_move_purchase_manifest) manifest_destinations
    LEFT JOIN releases_users_purchases rup_destination FORCE INDEX (idx_release_active)
      ON rup_destination.release_id=manifest_destinations.to_release_id
     AND rup_destination.is_active=1
    FOR UPDATE;


    SELECT COUNT(rm_lock.id) INTO locked_related_rows
    FROM release_move_purchase_manifest m_lock
    JOIN releases_migrations rm_lock ON rm_lock.id=m_lock.migration_id
    JOIN releases src_lock ON src_lock.id=m_lock.from_release_id
    JOIN releases dst_lock ON dst_lock.id=m_lock.to_release_id
    JOIN orders_items oi_lock ON oi_lock.id=m_lock.order_item_id
    JOIN orders o_lock ON o_lock.id=m_lock.order_id
    FOR UPDATE;
    IF locked_related_rows<>93 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Related migration, release, or order rows changed';
    END IF;

    SELECT SUM(
        CASE WHEN income_ids='' THEN 0
             ELSE 1 + LENGTH(income_ids) - LENGTH(REPLACE(income_ids, ',', ''))
        END
    ) INTO expected_income_rows
    FROM release_move_purchase_manifest;

    SELECT COUNT(income_lock.id) INTO locked_income_rows
    FROM release_move_purchase_manifest m_income
    LEFT JOIN income income_lock FORCE INDEX (i_sender_type_sender_date)
      ON income_lock.context_entity_type_id=21
     AND income_lock.context_entity_id=m_income.rup_id
    FOR UPDATE;
    IF locked_income_rows<>expected_income_rows THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Income row count changed from manifest';
    END IF;

    SELECT COUNT(DISTINCT rup_live.id) INTO live_candidate_rows

FROM releases_migrations rm_live
JOIN (SELECT DISTINCT migration_id FROM release_move_purchase_manifest) approved_migrations
  ON approved_migrations.migration_id=rm_live.id
JOIN releases src_live ON src_live.id=rm_live.from_release_id
JOIN releases dst_live ON dst_live.id=rm_live.to_release_id
JOIN releases_users_purchases rup_live ON rup_live.release_id=src_live.id AND rup_live.is_active=1
JOIN orders_items oi_live ON oi_live.context_entity_type_id=21 AND oi_live.context_entity_id=rup_live.id
JOIN orders o_live ON o_live.id=oi_live.order_id
LEFT JOIN releases_users_purchases dst_rup_live
  ON dst_rup_live.release_id=dst_live.id
 AND dst_rup_live.user_id=rup_live.user_id
 AND dst_rup_live.is_active=1

WHERE rm_live.is_active=1 AND rm_live.release_migration_type_id=1
  AND src_live.is_visible=0
  AND dst_live.is_visible=1 AND dst_live.is_published=1 AND dst_live.is_deleted=0
  AND oi_live.is_active=1 AND oi_live.order_item_status_id=3
  AND oi_live.order_item_type_id IN (8,10,11)
  AND o_live.order_status_id=3
  AND dst_rup_live.id IS NULL
;
    IF live_candidate_rows<>93 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Live candidate count changed; expected 93';
    END IF;

    SELECT COUNT(DISTINCT rup_live.id) INTO unmanifested_rows

FROM releases_migrations rm_live
JOIN (SELECT DISTINCT migration_id FROM release_move_purchase_manifest) approved_migrations
  ON approved_migrations.migration_id=rm_live.id
JOIN releases src_live ON src_live.id=rm_live.from_release_id
JOIN releases dst_live ON dst_live.id=rm_live.to_release_id
JOIN releases_users_purchases rup_live ON rup_live.release_id=src_live.id AND rup_live.is_active=1
JOIN orders_items oi_live ON oi_live.context_entity_type_id=21 AND oi_live.context_entity_id=rup_live.id
JOIN orders o_live ON o_live.id=oi_live.order_id
LEFT JOIN releases_users_purchases dst_rup_live
  ON dst_rup_live.release_id=dst_live.id
 AND dst_rup_live.user_id=rup_live.user_id
 AND dst_rup_live.is_active=1

LEFT JOIN release_move_purchase_manifest manifest_match ON manifest_match.rup_id=rup_live.id

WHERE rm_live.is_active=1 AND rm_live.release_migration_type_id=1
  AND src_live.is_visible=0
  AND dst_live.is_visible=1 AND dst_live.is_published=1 AND dst_live.is_deleted=0
  AND oi_live.is_active=1 AND oi_live.order_item_status_id=3
  AND oi_live.order_item_type_id IN (8,10,11)
  AND o_live.order_status_id=3
  AND dst_rup_live.id IS NULL

  AND manifest_match.rup_id IS NULL;
    IF unmanifested_rows<>0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Live candidate set contains rows outside the manifest';
    END IF;

    SELECT COUNT(*) INTO invalid_rows

FROM release_move_purchase_manifest m
LEFT JOIN releases_migrations rm ON rm.id=m.migration_id
LEFT JOIN releases src ON src.id=m.from_release_id
LEFT JOIN releases dst ON dst.id=m.to_release_id
LEFT JOIN releases_users_purchases rup ON rup.id=m.rup_id
LEFT JOIN orders_items oi ON oi.id=m.order_item_id
LEFT JOIN orders o ON o.id=m.order_id
LEFT JOIN (
    SELECT context_entity_id, GROUP_CONCAT(id ORDER BY id SEPARATOR ',') income_ids
    FROM income
    WHERE context_entity_type_id=21
    GROUP BY context_entity_id
) inc ON inc.context_entity_id=m.rup_id

    WHERE

       rm.id IS NULL OR rm.is_active<>1 OR rm.release_migration_type_id<>1
       OR rm.from_release_id<>m.from_release_id OR rm.to_release_id<>m.to_release_id
       OR src.id IS NULL OR src.is_visible<>0
       OR dst.id IS NULL OR dst.is_visible<>1 OR dst.is_published<>1 OR dst.is_deleted<>0
       OR rup.id IS NULL OR rup.user_id<>m.user_id OR rup.is_active<>1
       OR rup.net_price<>m.net_price OR rup.is_downloadable<>m.is_downloadable
       OR oi.id IS NULL OR oi.order_id<>m.order_id OR oi.context_entity_type_id<>21
       OR oi.context_entity_id<>m.rup_id OR oi.order_item_type_id<>m.order_item_type_id
       OR oi.order_item_status_id<>3 OR oi.is_active<>1
       OR o.id IS NULL OR o.order_status_id<>3
       OR COALESCE(inc.income_ids, '')<>m.income_ids
       OR (SELECT COUNT(*) FROM orders_items oi_count
           JOIN orders o_count ON o_count.id=oi_count.order_id
           WHERE oi_count.context_entity_type_id=21
             AND oi_count.context_entity_id=m.rup_id
             AND oi_count.order_item_type_id IN (8,10,11)
             AND oi_count.order_item_status_id=3
             AND oi_count.is_active=1
             AND o_count.order_status_id=3)<>1

       OR rup.release_id<>m.from_release_id
       OR EXISTS (
           SELECT 1 FROM releases_users_purchases duplicate_rup
           WHERE duplicate_rup.release_id=m.to_release_id
             AND duplicate_rup.user_id=m.user_id
             AND duplicate_rup.is_active=1
             AND duplicate_rup.id<>m.rup_id
       );

    IF invalid_rows<>0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Live data no longer matches the approved manifest';
    END IF;

    UPDATE releases_users_purchases rup
    JOIN release_move_purchase_manifest m ON m.rup_id=rup.id
    SET rup.release_id=m.to_release_id
    WHERE rup.release_id=m.from_release_id;
    SET updated_rows=ROW_COUNT();

    IF updated_rows<>93 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Updated row count changed; expected 93';
    END IF;

    SELECT COUNT(*) INTO invalid_rows
    FROM release_move_purchase_manifest m
    JOIN releases_users_purchases rup ON rup.id=m.rup_id
    WHERE rup.release_id<>m.to_release_id
       OR rup.user_id<>m.user_id
       OR rup.is_active<>1;
    IF invalid_rows<>0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Post-update entitlement verification failed';
    END IF;
END
