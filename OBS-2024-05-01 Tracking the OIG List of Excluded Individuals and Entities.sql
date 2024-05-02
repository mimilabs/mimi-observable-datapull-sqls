-- Databricks notebook source
SELECT excl_description, excldate, COUNT(*) as cnt 
FROM mimi_ws_1.hhsoig.leie 
GROUP BY excl_description, excldate;

-- COMMAND ----------


