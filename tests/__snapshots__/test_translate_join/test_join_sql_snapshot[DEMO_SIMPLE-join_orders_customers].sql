SELECT
  `t0`.`order_id` AS `order_id`,
  `t0`.`customer_id` AS `customer_id`,
  `t0`.`order_date` AS `order_date`,
  `t0`.`amount` AS `amount`,
  `t0`.`status` AS `status`,
  `t0`.`amount_eur` AS `amount_eur`,
  `t0`.`original_dataset` AS `original_dataset`,
  `t1`.`name` AS `name`,
  `t1`.`segment` AS `segment`
FROM `orders_all` AS `t0`
LEFT JOIN `customers` AS `t1`
  ON `t0`.`customer_id` = `t1`.`customer_id`
