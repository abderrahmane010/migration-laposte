SELECT
  `depot_id`,
  CAST(COUNT(CASE WHEN `status_norm` = 'DELIVERED' THEN 1 END) AS BIGINT) AS `DELIVERED_count`,
  CAST(COUNT(CASE WHEN `status_norm` = 'FAILED' THEN 1 END) AS BIGINT) AS `FAILED_count`
FROM `events_clean`
GROUP BY
  `depot_id`
