WITH `step_0` AS (
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
  FROM `events_anomalies`
), `step_1` AS (
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
  FROM `step_0`
  WHERE
    COALESCE(
      (
        (
          (
            `delay_minutes`
          ) + (
            0
          )
        ) > (
          0
        )
      ),
      FALSE
    )
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
FROM `step_1`
