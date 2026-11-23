WITH `step_0` AS (
  SELECT
    `order_id`,
    `customer_id`,
    `order_date`,
    `amount`,
    COALESCE(NULLIF(`status`, ''), 'UNKNOWN') AS `status`
  FROM `orders_raw`
), `step_1` AS (
  SELECT
    `order_id`,
    `customer_id`,
    `order_date`,
    `amount`,
    `status`,
    FLOOR((
      (
        `amount`
      ) * (
        CAST(1.0 AS DOUBLE)
      )
    ) * POWER(10, 2) + 0.5) / POWER(10, 2) AS `amount_eur`
  FROM `step_0`
), `step_2` AS (
  SELECT
    `order_id`,
    `customer_id`,
    `order_date`,
    `amount`,
    `status`,
    `amount_eur`
  FROM `step_1`
  WHERE
    NOT COALESCE((
      `status` IN ('CANCELLED')
    ), FALSE)
)
SELECT
  `order_id`,
  `customer_id`,
  `order_date`,
  `amount`,
  `status`,
  `amount_eur`
FROM `step_2`
