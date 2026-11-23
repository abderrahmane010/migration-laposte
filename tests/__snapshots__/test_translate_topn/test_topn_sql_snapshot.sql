SELECT
  `depot_id`,
  `delay_minutes_avg`
FROM (
  SELECT
    `depot_id`,
    `delay_minutes_avg`,
    `delay_minutes_avg_lag1`,
    `rank`,
    ROW_NUMBER() OVER (
      ORDER BY `delay_minutes_avg` DESC, `depot_id` ASC NULLS LAST, `delay_minutes_avg_lag1` ASC NULLS LAST, `rank` ASC NULLS LAST
    ) AS `__dku2dbx_first`
  FROM `depot_trend`
) AS `ranked`
WHERE
  `__dku2dbx_first` <= 10
