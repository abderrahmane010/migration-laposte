SELECT
  `route_id`,
  CAST(COUNT(*) AS BIGINT) AS `count`,
  CAST(SUM(`route_km`) AS DOUBLE) AS `route_km_sum`
FROM `delivered_routes`
GROUP BY
  `route_id`
