-- Databricks notebook source
SELECT measure_abbreviation, ffy, COUNT(*) FROM mimi_ws_1.datamedicaidgov.quality GROUP BY measure_abbreviation, ffy;

-- COMMAND ----------

SELECT measure_abbreviation, COUNT(DISTINCT ffy) FROM mimi_ws_1.datamedicaidgov.quality GROUP BY measure_abbreviation;

-- COMMAND ----------

SELECT measure_abbreviation, COUNT(*) FROM mimi_ws_1.datamedicaidgov.quality WHERE population = 'Medicaid' GROUP BY measure_abbreviation

-- COMMAND ----------

SELECT * FROM mimi_ws_1.datamedicaidgov.quality WHERE measure_abbreviation = 'BCS-AD' and population = 'Medicaid';

-- COMMAND ----------


