-- events_delivered
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
FROM `events_clean`
WHERE
  `status_norm` IN ('DELIVERED')

-- events_failed
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
FROM `events_clean`
WHERE
  `status_norm` IN ('FAILED')

-- events_other
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
FROM `events_clean`
WHERE
  `status_norm` IS NULL OR NOT (
    `status_norm` IN ('DELIVERED', 'FAILED')
  )
