/* Synthetic SQL recipe (fixture). */
SELECT
  a.depot_id,
  d.depot_name,
  COUNT(*) AS anomalies,
  MAX(a.event_ts) AS last_anomaly_ts
FROM `anomalies_clean` AS `a`
LEFT JOIN `depots_clean` AS `d`
  ON a.depot_id = d.depot_id
WHERE
  a.event_ts >= '2026-01-01'
GROUP BY
  a.depot_id,
  d.depot_name
