-- Databricks notebook source
/*
This SQL script uses the Open Payments data (mimi_ws_1.openpayments) to understand the relationship between medical specialty and types of payments. 
The output data is used in this Observable plot: https://observablehq.com/d/e5bbadae962c03d7
Take a look at the Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- In what situations would you need this type of information?
*/

SELECT nature_of_payment_or_transfer_of_value, 
    concat_ws('|', split_part(covered_recipient_specialty_1, "|", 2),
                split_part(covered_recipient_specialty_1, "|", 3)) as specialty, 
    AVG(total_amount_of_payment_usdollars) AS avg_amt,
    COUNT(*) AS cnt
FROM (SELECT * FROM general WHERE covered_recipient_npi IS NOT NULL)
GROUP BY nature_of_payment_or_transfer_of_value, 
    specialty
    HAVING avg_amt > 5000 AND cnt > 30;
