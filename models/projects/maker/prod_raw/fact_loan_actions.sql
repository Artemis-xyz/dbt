{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_loan_actions"
    )
}}


SELECT 
    la.ts,
    la.hash,
    il.asset_code AS code,
    SUM(la.dart) AS value,
    la.ilk
FROM {{ ref('fact_loan_actions_2') }} la
LEFT JOIN {{ ref('dim_ilk_list_labeled') }} il
    ON la.ilk = il.ilk
    AND CAST(la.ts AS DATE) BETWEEN COALESCE(il.begin_dt, '2000-01-01') AND COALESCE(il.end_dt, '2222-12-31')
GROUP BY la.ts, la.hash, il.asset_code, la.ilk
-- HAVING SUM(la.dart * la.rate) / POW(10, 45) != 0

UNION ALL

SELECT 
    ts,
    hash,
    21120 AS code,
    SUM(dart) AS value,
    ilk
FROM {{ ref('fact_loan_actions_2') }}
GROUP BY ts, hash, ilk
HAVING SUM(dart) != 0
