{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_interest_accruals_1"
    )
}}

SELECT 
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    dart,
    CAST(NULL AS NUMBER) AS rate
FROM ethereum_flipside.maker.fact_vat_frob
WHERE dart != 0

UNION ALL

SELECT 
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    dart/1e18,
    0 AS rate
FROM {{ ref('fact_vat_grab')}}
WHERE dart != 0

UNION ALL

SELECT 
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    CAST(NULL AS NUMBER) AS dart,
    rate
FROM ethereum_flipside.maker.fact_vat_fold
WHERE rate != 0