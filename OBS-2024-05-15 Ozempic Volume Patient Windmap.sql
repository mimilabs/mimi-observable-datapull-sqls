-- Databricks notebook source
/*
This SQL script uses the longitudinal Medicare Utilization data to track the number of members and claims for Ozempic usage.
The output data is used in this Observable plot: 

Take a look at these Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script. Also, please read the column/table descriptions of the tables used in the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- What are the limitations of this analysis? Does this script differentiate new grads?
- In what situations would you need this type of information?
*/

SELECT * FROM 
(SELECT citystate, 
  tot_clms, 
  LAG(tot_clms) OVER (PARTITION BY citystate ORDER BY _input_file_date ASC) AS tot_clms_prev,
  tot_benes,
  LAG(tot_benes) OVER (PARTITION BY citystate ORDER BY _input_file_date ASC) AS tot_benes_prev,
  _input_file_date
FROM (SELECT citystate, 
    _input_file_date,
    SUM(tot_clms) AS tot_clms, 
    SUM(tot_benes) AS tot_benes
  FROM (SELECT *, CONCAT(prscrbr_city, " in ", prscrbr_state_abrvtn) as citystate 
      FROM mimi_ws_1.datacmsgov.mupdpr
      WHERE brnd_name = 'Ozempic' AND _input_file_date in ('2020-12-31', '2021-12-31'))
  GROUP BY citystate, _input_file_date))
WHERE tot_clms_prev IS NOT NULL
ORDER BY tot_clms DESC LIMIT 50;

-- COMMAND ----------


