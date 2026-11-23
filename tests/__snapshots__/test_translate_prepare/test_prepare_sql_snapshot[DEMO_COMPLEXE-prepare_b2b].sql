WITH `step_0` AS (
  SELECT
    `customer_id`,
    `siret`,
    TRIM(`company`) AS `company`
  FROM `customers_b2b`
), `step_1` AS (
  SELECT
    `customer_id`,
    COALESCE(NULLIF(`siret`, ''), 'N/A') AS `siret`,
    `company`
  FROM `step_0`
)
SELECT
  `customer_id`,
  `siret`,
  `company`
FROM `step_1`
