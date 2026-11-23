SELECT
  `t0`.`event_id` AS `event_id`,
  `t0`.`parcel_id` AS `parcel_id`,
  `t0`.`depot_id` AS `depot_id`,
  `t0`.`customer_id` AS `customer_id`,
  `t0`.`event_ts` AS `event_ts`,
  `t0`.`status` AS `status`,
  `t0`.`delay_minutes` AS `delay_minutes`,
  `t0`.`amount_eur` AS `amount_eur`,
  `t1`.`route_id` AS `route_id`,
  `t1`.`route_km` AS `route_km`
FROM `delivered_enriched` AS `t0`
INNER JOIN `routes_ref` AS `t1`
  ON `t0`.`depot_id` = `t1`.`depot_id`
