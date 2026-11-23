SELECT
  `depot_id`,
  `delay_minutes_avg`
FROM `depot_trend`
ORDER BY
  `depot_id` ASC NULLS LAST,
  `delay_minutes_avg` DESC
