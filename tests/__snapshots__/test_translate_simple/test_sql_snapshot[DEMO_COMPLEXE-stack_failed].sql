SELECT
  `event_id`,
  `parcel_id`,
  `depot_id`,
  `customer_id`,
  `event_ts`,
  `status`,
  `delay_minutes`,
  `amount_eur`,
  `status_norm`,
  'events_failed' AS `original_dataset`
FROM `events_failed`
UNION ALL
SELECT
  `event_id`,
  `parcel_id`,
  `depot_id`,
  `customer_id`,
  `event_ts`,
  `status`,
  `delay_minutes`,
  `amount_eur`,
  `status_norm`,
  'events_other' AS `original_dataset`
FROM `events_other`
