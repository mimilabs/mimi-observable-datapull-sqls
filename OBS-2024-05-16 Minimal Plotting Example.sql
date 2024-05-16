-- Databricks notebook source
-- Count the number of records in the npidata table grouped by the _input_file_date column
SELECT _input_file_date, COUNT(*) 
FROM (
    -- Select all records from the npidata table where entity_type_code is '1'
    SELECT * FROM mimi_ws_1.nppes.npidata WHERE entity_type_code = '1'
)
GROUP BY _input_file_date;

