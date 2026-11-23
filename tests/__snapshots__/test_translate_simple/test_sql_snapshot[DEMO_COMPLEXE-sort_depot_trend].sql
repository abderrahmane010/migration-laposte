SELECT
  `depot_id`,
  `day`,
  `delay_minutes_avg`
FROM `depot_trend`
ORDER BY
  `depot_id` ASC NULLS LAST,
  `day` DESC
