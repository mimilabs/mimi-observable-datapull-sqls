-- Databricks notebook source
SELECT * FROM (
    SELECT date, 
        state, 
        enrollment, 
        LAG(enrollment, 1, 0) OVER (PARTITION BY state ORDER BY date ASC) AS enrollment_prev,
        (enrollment/enrollment_prev * 100 - 100) AS growth_rate
    FROM (
        SELECT _input_file_date AS date, state, SUM(enrollment) as enrollment 
        FROM (
            SELECT * FROM mimi_ws_1.partcd.cpsc_combined 
            WHERE parent_organization ILIKE "UnitedHealth%" AND
            plan_type != 'Medicare Prescription Drug Plan')
        GROUP BY _input_file_date, state)
) WHERE growth_rate IS NOT NULL;

-- COMMAND ----------

SELECT date, 
    enrollment, 
    LAG(enrollment, 1, 0) OVER (ORDER BY date ASC) AS enrollment_prev,
    (enrollment/enrollment_prev * 100 - 100) AS growth_rate
FROM (
    SELECT _input_file_date AS date, SUM(enrollment) as enrollment 
    FROM (
        SELECT * FROM mimi_ws_1.partcd.cpsc_combined 
        WHERE parent_organization ILIKE "UnitedHealth%" AND
        plan_type != 'Medicare Prescription Drug Plan')
    GROUP BY _input_file_date);

-- COMMAND ----------

SELECT * FROM (
    SELECT date, 
        contract_id, 
        enrollment, 
        LAG(enrollment, 1, 0) OVER (PARTITION BY contract_id ORDER BY date ASC) AS enrollment_prev,
        (enrollment/enrollment_prev * 100 - 100) AS growth_rate
    FROM (
        SELECT _input_file_date AS date, contract_id, SUM(enrollment) as enrollment 
        FROM (
            SELECT * FROM mimi_ws_1.partcd.cpsc_combined 
            WHERE parent_organization ILIKE "UnitedHealth%" AND
            plan_type != 'Medicare Prescription Drug Plan')
        GROUP BY _input_file_date, contract_id)
) WHERE growth_rate IS NOT NULL;

-- COMMAND ----------

SELECT * FROM (
SELECT date, 
    fips_state_county_code, 
    enrollment, 
    LAG(enrollment, 1, 0) OVER (PARTITION BY fips_state_county_code ORDER BY date ASC) AS enrollment_prev,
    (enrollment - enrollment_prev) AS growth_delta
FROM (
    SELECT _input_file_date AS date, fips_state_county_code, SUM(enrollment) as enrollment 
    FROM (
        SELECT * FROM mimi_ws_1.partcd.cpsc_combined 
        WHERE parent_organization ILIKE "UnitedHealth%" AND
        plan_type != 'Medicare Prescription Drug Plan' AND
        _input_file_date in ('2024-01-01', '2024-05-01'))
    GROUP BY _input_file_date, fips_state_county_code)) WHERE date = '2024-05-01';


-- COMMAND ----------


