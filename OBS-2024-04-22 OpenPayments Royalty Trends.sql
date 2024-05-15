-- Databricks notebook source
/*
This SQL script uses the Open Payments data (mimi_ws_1.openpayments) to track the royalty and license fees for doctors and hospitals.
The output data is used in this Observable plot: https://observablehq.com/d/aebe213f0805e139
Take a look at the Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- In what situations would you need this type of information?
*/
CREATE TEMPORARY VIEW id2name
  AS (SELECT id, FIRST(_name) as name 
        FROM (SELECT coalesce(teaching_hospital_ccn, 
                      covered_recipient_npi) AS id,
          coalesce(teaching_hospital_name,
                  concat_ws(' ', covered_recipient_first_name, 
                                covered_recipient_last_name)) as _name
          FROM mimi_ws_1.openpayments.general) GROUP BY id);

SELECT * FROM (SELECT coalesce(teaching_hospital_ccn, 
                      covered_recipient_npi) AS id,
  total_amount_of_payment_usdollars, date_of_payment, 
  form_of_payment_or_transfer_of_value, 
  name_of_drug_or_biological_or_device_or_medical_supply_1,
  applicable_manufacturer_or_applicable_gpo_making_payment_name 
FROM mimi_ws_1.openpayments.general 
WHERE nature_of_payment_or_transfer_of_value = 'Royalty or License' AND 
  indicate_drug_or_biological_or_device_or_medical_supply_1 in ('Drug', 'Biological')) AS A
LEFT JOIN id2name AS B ON A.id = B.id;

-- COMMAND ----------


