{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_interest_accruals_3"
    )
}}

SELECT 
    ilk,
    ts,
    hash,
    SUM(cumulative_dart * rate * 10) AS interest_accruals
FROM {{ ref('fact_interest_accruals_2') }}
WHERE rate IS NOT NULL
GROUP BY ilk, ts, hash