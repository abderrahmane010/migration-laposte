SELECT
  `depot_id`,
  CAST(COUNT(*) AS BIGINT) AS `count`,
  CAST(AVG(`delay_minutes`) AS DOUBLE) AS `delay_minutes_avg`,
  CAST(SUM(`amount_eur`) AS DOUBLE) AS `amount_eur_sum`
FROM `delivered_enriched`
GROUP BY
  `depot_id`
