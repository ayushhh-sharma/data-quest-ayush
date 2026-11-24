-- Gold: three assignment answers. SQL is primary. Reads silver current only.
-- Q2: quarterly series use Q01-Q04. Series with no quarters use Q05. Latest year on a tie.

CREATE OR REFRESH MATERIALIZED VIEW pr_population.gold.q1_population_2013_2018
COMMENT 'Q1. Mean and standard deviation of annual US population, 2013-2018 inclusive. stddev_sample is the reported answer.'
AS
SELECT
  'United States' AS nation,
  2013 AS year_start,
  2018 AS year_end,
  count(*) AS n_years,
  avg(population) AS mean_population,
  stddev_samp(population) AS stddev_sample,
  stddev_pop(population) AS stddev_population
FROM pr_population.silver.population
WHERE nation = 'United States'
  AND year BETWEEN 2013 AND 2018;


CREATE OR REFRESH MATERIALIZED VIEW pr_population.gold.q2_series_best_year
COMMENT 'Q2. Best year per series_id: largest quarterly sum, or Q05 if the series has no quarters. Latest year on a tie.'
AS
WITH year_sum AS (
  SELECT series_id, year, sum(value) AS year_value
  FROM pr_population.silver.pr_all_data
  WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
  GROUP BY series_id, year
  UNION ALL
  SELECT series_id, year, sum(value) AS year_value
  FROM pr_population.silver.pr_all_data
  WHERE period = 'Q05'
    AND series_id NOT IN (
      SELECT DISTINCT series_id
      FROM pr_population.silver.pr_all_data
      WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
    )
  GROUP BY series_id, year
)
SELECT
  y.series_id,
  l.series_title,
  y.year AS best_year,
  y.year_value
FROM year_sum y
LEFT JOIN pr_population.silver.pr_series_label l ON y.series_id = l.series_id
QUALIFY row_number() OVER (
  PARTITION BY y.series_id
  ORDER BY y.year_value DESC, y.year DESC
) = 1;


CREATE OR REFRESH MATERIALIZED VIEW pr_population.gold.q3_prs30006032_q01
COMMENT 'Q3. PRS30006032 Q01 value by year, left-joined to that year population. Years with no population stay.'
AS
SELECT
  a.series_id,
  a.period,
  a.year,
  a.value,
  p.population
FROM pr_population.silver.pr_all_data a
LEFT JOIN pr_population.silver.population p ON a.year = p.year
WHERE a.series_id = 'PRS30006032'
  AND a.period = 'Q01';
