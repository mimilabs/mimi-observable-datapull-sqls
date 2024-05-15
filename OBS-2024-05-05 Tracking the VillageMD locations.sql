-- Databricks notebook source
-- VillageMD

/*
This SQL script uses the NPPES database to track the growth of VillageMD. 
To find the NPI numbers for VillageMD, we used this tool: https://npi-db.org
The output data is used in this Observable plot: https://observablehq.com/d/80aa9ef206f8f868

Take a look at these Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- In what situations would you need this type of information?
*/

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


