-- Databricks notebook source
SELECT *
FROM (
    SELECT _input_file_date, entity_type_code, COUNT(*) AS cnt
    FROM mimi_ws_1.nppes.npidata
    GROUP BY _input_file_date, entity_type_code
) AS subquery
WHERE entity_type_code = '1'
ORDER BY _input_file_date ASC;

-- COMMAND ----------


