WITH
  P1 AS(
  SELECT
    scenario_id,
    item_id,
    site_id,
    period_id,
    avg_price,
    shelf_price,
    ((shelf_price-avg_price)/shelf_price)AS Discount,
    LAG(avg_price) OVER(PARTITION BY scenario_id, item_id, site_id ORDER BY period_id ASC)AS avg_price_lag_1,
    LAG(avg_price,2) OVER(PARTITION BY scenario_id, item_id, site_id ORDER BY period_id ASC) avg_price_lag_2,
    LAG(avg_price,3) OVER(PARTITION BY scenario_id, item_id, site_id ORDER BY period_id ASC) avg_price_lag_3,
    LAG(avg_price,4) OVER(PARTITION BY scenario_id, item_id, site_id ORDER BY period_id ASC) avg_price_lag_4,
    LAG(avg_price,5) OVER(PARTITION BY scenario_id, item_id, site_id ORDER BY period_id ASC) avg_price_lag_5,
    AVG(avg_price) OVER(PARTITION BY scenario_id, item_id, site_id ORDER BY period_id ASC) avg_price_5w_mean
  FROM
    `gcp-wow-supers-rtlapsim-dev.ngp_sample.dynamic_lag_input` ),
  P2 AS (
  SELECT
    N.*,
    M.Discount AS PD
  FROM
    P1 N
  LEFT JOIN
    P1 M
  ON
    N.scenario_id=M.scenario_id
    AND N.item_id=M.item_id
    AND N.site_id=M.site_id
    AND M.period_id BETWEEN N.period_id-4
    AND N.period_id-1 )
SELECT
  scenario_id,
  item_id,
  site_id,
  period_id,
  SUM(CASE
      WHEN (PD>=0.3)AND (PD>=Discount) THEN 1
    ELSE
    0
  END
    ) AS Num_Of_discount,
  SUM(avg_price_lag_1)as avg_price_lag1,
  SUM(avg_price_lag_2)as avg_price_lag2,
  SUM(avg_price_lag_3)as avg_price_lag3,
  SUM(avg_price_lag_4)as avg_price_lag3,
  SUM(avg_price_lag_5)as avg_price_lag4,
  SUM(avg_price_5w_mean)as avg_price_5w_mean,
FROM
  P2
GROUP BY
  scenario_id,
  item_id,
  site_id,
  period_id
ORDER BY
  scenario_id,
  item_id,
  site_id,
  period_id