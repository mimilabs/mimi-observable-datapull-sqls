-- Databricks notebook source
SELECT nature_of_payment_or_transfer_of_value, 
    concat_ws('|', split_part(covered_recipient_specialty_1, "|", 2),
                split_part(covered_recipient_specialty_1, "|", 3)) as specialty, 
    AVG(total_amount_of_payment_usdollars) AS tot_amt,
    COUNT(*) AS cnt
FROM (SELECT * FROM general WHERE covered_recipient_npi IS NOT NULL)
GROUP BY nature_of_payment_or_transfer_of_value, 
    specialty
    HAVING tot_amt > 5000 AND cnt > 30;
