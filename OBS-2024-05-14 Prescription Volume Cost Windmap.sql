-- Databricks notebook source
-- 
SELECT * FROM 
(SELECT brnd_name, 
  tot_clms, 
  LAG(tot_clms) OVER (PARTITION BY brnd_name ORDER BY _input_file_date) AS tot_clms_prev,
  tot_drug_cst,
  LAG(tot_drug_cst) OVER (PARTITION BY brnd_name ORDER BY _input_file_date) AS tot_drug_cst_prev,
  _input_file_date
FROM mimi_ws_1.datacmsgov.mupdpr_geo 
WHERE prscrbr_geo_lvl = 'National'
  AND tot_drug_cst > 100000000
  AND _input_file_date in ('2020-12-31', '2021-12-31'))
WHERE tot_clms_prev IS NOT NULL;

-- COMMAND ----------


