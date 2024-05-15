-- Databricks notebook source
/*
This SQL script uses the longitudinal NPPES databases to track physician migrations.
The output data is used in this Observable plot: https://observablehq.com/d/85fe974752b841e2

Take a look at these Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script. Also, please read the column/table descriptions of the tables used in the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- What are the limitations of this analysis? Does this script differentiate new grads?
- In what situations would you need this type of information?
*/

SELECT state23, state24, COUNT(*) as cnt FROM (
  WITH nppes20230409 AS (
    SELECT npi, provider_business_practice_location_address_state_name 
    FROM mimi_ws_1.nppes.npidata
    WHERE _input_file_date = '2023-04-09' AND entity_type_code = '1')
  SELECT 
    a.npi, 
    a.provider_business_practice_location_address_state_name AS state24,  
    b.provider_business_practice_location_address_state_name AS state23
  FROM (
    SELECT npi, provider_business_practice_location_address_state_name 
    FROM mimi_ws_1.nppes.npidata
    WHERE _input_file_date = '2024-04-07' 
      AND entity_type_code = '1' 
      AND (CONTAINS(healthcare_provider_taxonomies, '207Q00000X') OR
        CONTAINS(healthcare_provider_taxonomies, '208D00000X') OR
        CONTAINS(healthcare_provider_taxonomies, '207R00000X'))
  ) AS a
  LEFT JOIN nppes20230409 AS b ON a.npi = b.npi
  WHERE a.provider_business_practice_location_address_state_name != b.provider_business_practice_location_address_state_name)
GROUP BY state23, state24;

-- COMMAND ----------

SELECT DISTINCT _input_file_date FROM mimi_ws_1.nppes.npidata;
