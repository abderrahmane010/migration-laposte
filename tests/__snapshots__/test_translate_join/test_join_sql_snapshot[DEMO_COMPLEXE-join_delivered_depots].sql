SELECT
  `t0`.`event_id` AS `event_id`,
  `t0`.`parcel_id` AS `parcel_id`,
  `t0`.`depot_id` AS `depot_id`,
  `t0`.`customer_id` AS `customer_id`,
  `t0`.`event_ts` AS `event_ts`,
  `t0`.`status` AS `status`,
  `t0`.`delay_minutes` AS `delay_minutes`,
  `t0`.`amount_eur` AS `amount_eur`,
  `t0`.`status_norm` AS `status_norm`,
  `t1`.`depot_name` AS `depot_name`,
  `t1`.`department` AS `department`
FROM `events_delivered` AS `t0`
LEFT JOIN `depots_clean` AS `t1`
  ON `t0`.`depot_id` = `t1`.`depot_id`
