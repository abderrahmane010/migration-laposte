SELECT
  `depot_id`,
  `delay_minutes_avg`,
  CAST(LAG(`delay_minutes_avg`, 1) OVER (ORDER BY `delay_minutes_avg` DESC) AS DOUBLE) AS `delay_minutes_avg_lag1`,
  CAST(RANK() OVER (ORDER BY `delay_minutes_avg` DESC) AS BIGINT) AS `rank`
FROM `daily_depot_kpis`
