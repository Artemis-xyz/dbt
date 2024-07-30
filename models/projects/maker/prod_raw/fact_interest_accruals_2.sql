{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_interest_accruals_2"
    )
}}

SELECT 
    *,
    SUM(dart) OVER (PARTITION BY ilk ORDER BY ts) AS cumulative_dart
FROM {{ ref('fact_interest_accruals_1') }}