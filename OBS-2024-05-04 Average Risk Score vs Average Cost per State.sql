-- Databricks notebook source
/*
This SQL script uses the Medicare Utilization data to find the relationship between patient risk scores and hospital payments.
The output data is used in this Observable plot: https://observablehq.com/d/6b2c7542cb8d1308
Also, take a look at this forked plot by Evan Galloway as well: https://observablehq.com/d/853c9c8a9de80c5b#cell-21

Take a look at these Observable scripts and their descriptions to learn how the data was used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- In what situations would you need this type of information?
*/

SELECT 
  tot_mdcr_pymt_amt/tot_benes as avg_mdcr_pymt_amt, 
  bene_avg_risk_scre, 
  rndrng_prvdr_state_abrvtn,
  rndrng_prvdr_org_name,
  tot_dschrgs 
FROM mimi_ws_1.datacmsgov.mupihp_prvdr 
WHERE _input_file_date = '2019-12-31';

-- COMMAND ----------


