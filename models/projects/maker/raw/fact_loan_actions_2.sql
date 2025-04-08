{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_loan_actions_2"
    )
}}

SELECT 
    ilk,
    ts,
    hash,
    dart,
    COALESCE(POW(10,27) + SUM(rate) OVER(PARTITION BY ilk ORDER BY ts ASC), POW(10,27)) AS rate
FROM {{ ref('fact_interest_accruals_1') }}
WHERE ilk != 'TELEPORT-FW-A'