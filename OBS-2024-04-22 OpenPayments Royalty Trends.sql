-- Databricks notebook source
CREATE TEMPORARY VIEW id2name
  AS (SELECT id, FIRST(_name) as name 
        FROM (SELECT coalesce(teaching_hospital_ccn, 
                      covered_recipient_npi) AS id,
          coalesce(teaching_hospital_name,
                  concat_ws(' ', covered_recipient_first_name, 
                                covered_recipient_last_name)) as _name
          FROM general) GROUP BY id);

SELECT * FROM (SELECT coalesce(teaching_hospital_ccn, 
                      covered_recipient_npi) AS id,
  total_amount_of_payment_usdollars, date_of_payment, 
  form_of_payment_or_transfer_of_value, 
  name_of_drug_or_biological_or_device_or_medical_supply_1,
  applicable_manufacturer_or_applicable_gpo_making_payment_name 
FROM general 
WHERE nature_of_payment_or_transfer_of_value = 'Royalty or License' AND 
  indicate_drug_or_biological_or_device_or_medical_supply_1 in ('Drug', 'Biological')) AS A
LEFT JOIN id2name AS B ON A.id = B.id;
