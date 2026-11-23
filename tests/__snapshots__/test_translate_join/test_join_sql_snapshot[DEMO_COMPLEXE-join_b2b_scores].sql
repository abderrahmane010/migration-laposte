SELECT
  `t0`.`customer_id` AS `customer_id`,
  `t1`.`siret` AS `siret`,
  `t1`.`company` AS `company`,
  `t0`.`score` AS `score`,
  `t0`.`is_good` AS `is_good`
FROM `customer_scores` AS `t0`
INNER JOIN `b2b_clean` AS `t1`
  ON `t0`.`customer_id` = `t1`.`customer_id`
