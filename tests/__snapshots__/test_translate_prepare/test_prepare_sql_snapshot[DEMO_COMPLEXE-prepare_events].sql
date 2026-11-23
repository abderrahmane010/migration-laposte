WITH `step_0` AS (
  SELECT
    `event_id`,
    `parcel_id`,
    `depot_id`,
    `customer_id`,
    `event_ts`,
    `status`,
    `delay_minutes`,
    `amount_eur`
  FROM `events_sync`
), `step_1` AS (
  SELECT
    `event_id`,
    `parcel_id`,
    `depot_id`,
    `customer_id`,
    `event_ts`,
    UPPER(`status`) AS `status`,
    `delay_minutes`,
    `amount_eur`
  FROM `step_0`
), `step_2` AS (
  SELECT
    `event_id`,
    `parcel_id`,
    `depot_id`,
    `customer_id`,
    `event_ts`,
    `status`,
    `delay_minutes`,
    `amount_eur`,
    CASE WHEN (
      NULLIF(`status`, '')
    ) IS NULL THEN 'UNKNOWN' ELSE `status` END AS `status_norm`
  FROM `step_1`
), `step_3` AS (
  SELECT
    `event_id`,
    `parcel_id`,
    `depot_id`,
    `customer_id`,
    `event_ts`,
    `status`,
    COALESCE(`delay_minutes`, CAST(0 AS INT)) AS `delay_minutes`,
    `amount_eur`,
    `status_norm`
  FROM `step_2`
)
SELECT
  `event_id`,
  `parcel_id`,
  `depot_id`,
  `customer_id`,
  `event_ts`,
  `status`,
  `delay_minutes`,
  `amount_eur`,
  `status_norm`
FROM `step_3`
