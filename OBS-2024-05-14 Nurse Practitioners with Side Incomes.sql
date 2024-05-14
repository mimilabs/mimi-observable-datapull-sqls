-- Databricks notebook source
-- This query gets NPs who have multiple practice offices and have other side income from Pharma and Device companies
SELECT covered_recipient_npi, SUM(total_amount_of_payment_usdollars) AS tot_amt
FROM (
  SELECT * FROM mimi_ws_1.openpayments.general
  WHERE covered_recipient_npi in (
    SELECT npi
    FROM (
      SELECT npi, COUNT(*) AS pl_cnt
      FROM (SELECT a.npi AS npi, b.pl_token AS pl_token
        FROM mimi_ws_1.nppes.npidata AS a
        LEFT JOIN mimi_ws_1.nppes.pl_se AS b ON a.npi=b.npi
        WHERE CONTAINS(a.healthcare_provider_taxonomies, '363LF0000X') 
            AND a.provider_business_mailing_address_state_name = 'AZ'
            AND a.entity_type_code = '1'
            AND a._input_file_date > '2024-05-01')
      GROUP BY npi
      HAVING pl_cnt > 1))
) GROUP BY covered_recipient_npi;

-- COMMAND ----------


