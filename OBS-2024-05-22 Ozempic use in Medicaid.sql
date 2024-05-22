-- Databricks notebook source
/*
We just ingested a few datasets from the data.medicaid.gov
There, we found the state drug utilization datasets.
We are trying to play with the data.
First, let's see the Ozempic use.
*/
SELECT * 
FROM mimi_ws_1.datamedicaidgov.drugutilization 
WHERE CONTAINS(product_name, 'OZEMPIC')
  AND state != 'XX'
  AND suppression_used = false;

-- COMMAND ----------


