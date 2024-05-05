-- Databricks notebook source
-- VillageMD
SELECT *
FROM mimi_ws_1.nppes.npidata
WHERE _input_file_date = '2024-04-07' AND 
  entity_type_code = '2' AND
  healthcare_provider_taxonomies ILIKE '207Q00000X%' AND
  npi in (SELECT npi
    FROM mimi_ws_1.nppes.address_h3 
    WHERE h3_m in (
      SELECT h3_m 
      FROM mimi_ws_1.nppes.address_h3 
      WHERE npi = '1245687946'
    )
  );

-- COMMAND ----------

-- VillageMD providers working there...
SELECT * 
FROM mimi_ws_1.datacmsgov.reval
WHERE
  group_pac_id IN 
  (
    SELECT DISTINCT pecos_asct_cntl_id 
    FROM mimi_ws_1.datacmsgov.pc_provider
    WHERE npi IN
      (SELECT npi
        FROM mimi_ws_1.nppes.npidata
        WHERE _input_file_date = '2024-04-07' AND 
          healthcare_provider_taxonomies ILIKE '207Q00000X%' AND
          npi in (SELECT npi
            FROM mimi_ws_1.nppes.address_h3 
            WHERE h3_m in (
              SELECT h3_m 
              FROM mimi_ws_1.nppes.address_h3 
              WHERE npi = '1245687946'
            )
          )
      )
  );

-- COMMAND ----------


