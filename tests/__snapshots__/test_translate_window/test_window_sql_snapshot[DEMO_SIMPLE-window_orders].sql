SELECT
  `order_id`,
  `customer_id`,
  `order_date`,
  `amount`,
  `status`,
  `amount_eur`,
  CAST(RANK() OVER (PARTITION BY `customer_id` ORDER BY `order_date` ASC NULLS LAST) AS BIGINT) AS `rank`,
  CAST(SUM(`amount_eur`) OVER (
    PARTITION BY `customer_id`
    ORDER BY `order_date` ASC NULLS LAST
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS DOUBLE) AS `amount_eur_sum`
FROM `orders_enriched`
