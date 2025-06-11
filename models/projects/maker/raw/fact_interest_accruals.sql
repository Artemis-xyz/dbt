{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_interest_accruals"
    )
}}

WITH interest_accruals AS (
    SELECT 
        ia.ts,
        ia.hash,
        il.equity_code AS code,
        SUM(ia.interest_accruals) AS value,
        ia.ilk
    FROM {{ ref('fact_interest_accruals_3') }} ia
    LEFT JOIN {{ ref('dim_ilk_list_labeled') }} il
        ON ia.ilk = il.ilk
        AND ia.ts BETWEEN COALESCE(il.begin_dt, '2000-01-01') AND COALESCE(il.end_dt, '2222-12-31')
    GROUP BY ia.ts, ia.hash, il.equity_code, ia.ilk

    UNION ALL

    SELECT 
        ia.ts,
        ia.hash,
        il.asset_code AS code,
        SUM(ia.interest_accruals) AS value,
        ia.ilk
    FROM {{ ref('fact_interest_accruals_3') }} ia
    LEFT JOIN {{ ref('dim_ilk_list_labeled') }} il
        ON ia.ilk = il.ilk
        AND CAST(ia.ts AS DATE) BETWEEN COALESCE(il.begin_dt, '2000-01-01') AND COALESCE(il.end_dt, '2222-12-31')
    GROUP BY ia.ts, ia.hash, il.asset_code, ia.ilk
)

SELECT * FROM interest_accruals