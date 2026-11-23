SELECT
  `order_id`,
  `customer_id`,
  `order_date`,
  `amount`,
  `status`,
  `amount_eur`,
  'orders_prepared' AS `original_dataset`
FROM `orders_prepared`
UNION ALL
SELECT
  `order_id`,
  `customer_id`,
  `order_date`,
  `amount`,
  `status`,
  CAST(NULL AS DOUBLE) AS `amount_eur`,
  'orders_archive' AS `original_dataset`
FROM `orders_archive`
