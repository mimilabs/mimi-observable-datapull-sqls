-- Databricks notebook source
/*
This SQL script uses the longitudinal Medicare Utilization data to track the volume/total prices of prescription drugs.
The output data is used in this Observable plot: https://observablehq.com/d/f45096acdf076783

Take a look at these Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script. Also, please read the column/table descriptions of the tables used in the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- What are the limitations of this analysis? Does this script differentiate new grads?
- In what situations would you need this type of information?
*/

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


