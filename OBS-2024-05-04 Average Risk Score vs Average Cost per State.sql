-- Databricks notebook source
SELECT 
  tot_mdcr_pymt_amt/tot_benes as avg_mdcr_pymt_amt, 
  bene_avg_risk_scre, 
  rndrng_prvdr_state_abrvtn,
  rndrng_prvdr_org_name,
  tot_dschrgs 
FROM mimi_ws_1.datacmsgov.mupihp_prvdr 
WHERE _input_file_date = '2019-12-31';

-- COMMAND ----------


