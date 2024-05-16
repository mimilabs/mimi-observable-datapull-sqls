-- Databricks notebook source
WITH cpt99454 AS (SELECT negotiated_rate AS rate_99454,
  provider_reference,
  source
FROM mimi_ws_1.payermrf.in_network_rates 
WHERE billing_code='99454'
  AND negotiation_arrangement = 'ffs')

SELECT rate_99457, rate_99454, a.provider_reference
FROM (SELECT negotiated_rate AS rate_99457,
  provider_reference, 
  source
FROM mimi_ws_1.payermrf.in_network_rates
WHERE billing_code='99457'
  AND negotiation_arrangement = 'ffs') AS a
LEFT JOIN cpt99454 AS b 
ON a.provider_reference = b.provider_reference AND
a.source = b.source;

-- COMMAND ----------


