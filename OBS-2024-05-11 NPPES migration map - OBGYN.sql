-- Databricks notebook source
SELECT state22, state24, COUNT(*) as cnt FROM (
  WITH nppes20220508 AS (
    SELECT npi, provider_business_practice_location_address_state_name 
    FROM mimi_ws_1.nppes.npidata
    WHERE _input_file_date = '2022-04-10' AND entity_type_code = '1')
  SELECT 
    a.npi, 
    CASE 
      WHEN a.provider_business_practice_location_address_state_name IN ('AL', 'AR', 'ID', 'IN', 'KY', 'LA', 'MS', 'MO', 'ND', 'OK', 'SD', 'TN', 'WV', 'FL', 'GA', 'SC', 'NE', 'NC', 'AZ', 'UT') THEN 'BANNED'
      ELSE 'NOTBANNED' END AS state24,  
    CASE 
      WHEN b.provider_business_practice_location_address_state_name IN ('AL', 'AR', 'ID', 'IN', 'KY', 'LA', 'MS', 'MO', 'ND', 'OK', 'SD', 'TN', 'WV', 'FL', 'GA', 'SC', 'NE', 'NC', 'AZ', 'UT') THEN 'BANNED'
      ELSE 'NOTBANNED' END AS state22
  FROM (
    SELECT npi, provider_business_practice_location_address_state_name 
    FROM mimi_ws_1.nppes.npidata
    WHERE _input_file_date = '2024-04-07' 
      AND entity_type_code = '1' 
      AND (CONTAINS(healthcare_provider_taxonomies, '207V00000X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VC0200X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VF0040X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VX0201X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VG0400X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VH0002X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VM0101X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VB0002X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VX0000X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VE0102X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VC0300X'))
  ) AS a
  LEFT JOIN nppes20220508 AS b ON a.npi = b.npi
  WHERE a.provider_business_practice_location_address_state_name != b.provider_business_practice_location_address_state_name)
GROUP BY state22, state24;

-- COMMAND ----------

SELECT state22, state24, COUNT(*) as cnt FROM (
  WITH nppes20220508 AS (
    SELECT npi, provider_business_practice_location_address_state_name 
    FROM mimi_ws_1.nppes.npidata
    WHERE _input_file_date = '2018-04-08' AND entity_type_code = '1')
  SELECT 
    a.npi, 
    CASE 
      WHEN a.provider_business_practice_location_address_state_name IN ('AL', 'AR', 'ID', 'IN', 'KY', 'LA', 'MS', 'MO', 'ND', 'OK', 'SD', 'TN', 'WV', 'FL', 'GA', 'SC', 'NE', 'NC', 'AZ', 'UT') THEN 'BANNED'
      ELSE 'NOTBANNED' END AS state24,  
    CASE 
      WHEN b.provider_business_practice_location_address_state_name IN ('AL', 'AR', 'ID', 'IN', 'KY', 'LA', 'MS', 'MO', 'ND', 'OK', 'SD', 'TN', 'WV', 'FL', 'GA', 'SC', 'NE', 'NC', 'AZ', 'UT') THEN 'BANNED'
      ELSE 'NOTBANNED' END AS state22
  FROM (
    SELECT npi, provider_business_practice_location_address_state_name 
    FROM mimi_ws_1.nppes.npidata
    WHERE _input_file_date = '2020-04-12' 
      AND entity_type_code = '1' 
      AND (CONTAINS(healthcare_provider_taxonomies, '207V00000X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VC0200X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VF0040X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VX0201X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VG0400X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VH0002X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VM0101X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VB0002X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VX0000X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VE0102X') OR
      CONTAINS(healthcare_provider_taxonomies, '207VC0300X'))
  ) AS a
  LEFT JOIN nppes20220508 AS b ON a.npi = b.npi
  WHERE a.provider_business_practice_location_address_state_name != b.provider_business_practice_location_address_state_name)
GROUP BY state22, state24;

-- COMMAND ----------


