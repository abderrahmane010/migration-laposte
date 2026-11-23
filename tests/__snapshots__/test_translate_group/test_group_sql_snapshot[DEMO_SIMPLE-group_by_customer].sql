SELECT
  `customer_id`,
  CAST(COUNT(*) AS BIGINT) AS `count`,
  CAST(SUM(`amount_eur`) AS DOUBLE) AS `amount_eur_sum`,
  CAST(AVG(`amount_eur`) AS DOUBLE) AS `amount_eur_avg`
FROM `orders_enriched`
GROUP BY
  `customer_id`
