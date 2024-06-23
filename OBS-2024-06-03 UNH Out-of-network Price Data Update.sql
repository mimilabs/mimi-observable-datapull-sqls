-- Databricks notebook source
/*
This SQL script uses the Payer Price Transparency File to look at the Out-of-Network Prices and Volumes for Apple, Inc.
The output data is used in this Observable plot: https://observablehq.com/d/0aba42ab1a83913e

Many PT, Acupucture, and Pscychothepary procedures are found in the OON data. 

Take a look at these Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script. Also, please read the column/table descriptions of the tables used in the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- What are the limitations of this analysis? Does this script differentiate new grads?
- In what situations would you need this type of information?
*/

-- COMMAND ----------

SELECT billing_code, name, description, last_updated_on,
  COUNT(DISTINCT npi) as cnt_unq_prvdr, 
  (AVG(billed_charge) - AVG(allowed_amount)) AS potential_balance_billing,
  AVG(billed_charge) as avg_billed_charge,
  AVG(allowed_amount) as avg_allowed_amount
FROM (SELECT * 
  FROM mimi_ws_1.payermrf.allowed_amounts 
  WHERE employer = 'APPLE--INC')
GROUP BY billing_code, name, description, last_updated_on;

-- COMMAND ----------


