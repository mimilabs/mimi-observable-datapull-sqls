-- Databricks notebook source
SELECT _input_file_date AS date, plan_type, SUM(enrollment) as enrollment FROM (
    SELECT * FROM cpsc_combined WHERE parent_organization ILIKE "UnitedHealth%")
GROUP BY _input_file_date, plan_type;
