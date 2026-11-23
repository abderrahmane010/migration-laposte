SELECT
  `event_id`,
  `parcel_id`,
  `depot_id`,
  `customer_id`,
  `event_ts`,
  `status`,
  `delay_minutes`,
  `amount_eur`,
  `internal_flag`
FROM `parcels_events`
