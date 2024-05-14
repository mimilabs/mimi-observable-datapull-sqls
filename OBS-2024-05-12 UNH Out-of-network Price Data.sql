-- Databricks notebook source
SELECT billing_code, name, description,
  COUNT(DISTINCT npi) as cnt_unq_prvdr, 
  (AVG(billed_charge) - AVG(allowed_amount)) AS potential_balance_billing,
  AVG(billed_charge) as avg_billed_charge,
  AVG(allowed_amount) as avg_allowed_amount
FROM (SELECT * 
  FROM mimi_ws_1.payermrf.allowed_amounts 
  WHERE employer = 'APPLE--INC')
GROUP BY billing_code, name, description;

-- COMMAND ----------

SELECT npi, COUNT(*) as cnt, (AVG(billed_charge) - AVG(allowed_amount))
FROM (SELECT * 
  FROM mimi_ws_1.payermrf.allowed_amounts 
  WHERE employer = 'APPLE--INC')
GROUP BY npi;

-- COMMAND ----------

SELECT * FROM mimi_ws_1.payermrf.allowed_amounts WHERE billing_code = '97810' AND employer = 'APPLE--INC';

-- COMMAND ----------


