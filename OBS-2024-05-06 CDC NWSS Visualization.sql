-- Databricks notebook source


-- COMMAND ----------

-- NWSS COVID
SELECT county_names, date_end, AVG(percentile)
FROM (
  SELECT a.wwtp_id, a.county_names, a.date_end, a.percentile, b.latitude, b.longitude
  FROM mimi_ws_1.cdc.nwss_covid AS a
  LEFT JOIN
  (SELECT key_plot_id, latitude, longitude 
    FROM mimi_ws_1.cdc.nwss_mpox) AS b 
  ON a.key_plot_id=b.key_plot_id
  WHERE a.reporting_jurisdiction = 'California' AND
    b.latitude IS NOT NULL AND
    b.longitude IS NOT NULL AND
    a.percentile <= 100) 
GROUP BY county_names, date_end;

-- COMMAND ----------


