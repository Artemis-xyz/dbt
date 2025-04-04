{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_liquidation"
    )
}}

-- Liquidation Revenues
SELECT
    ts,
    hash,
    31210 AS code,
    value AS value
FROM {{ ref('fact_liquidation_revenue') }}

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value
FROM {{ ref('fact_liquidation_revenue') }}

UNION ALL

-- Liquidation Expenses
SELECT
    block_timestamp as ts,
    tx_hash as hash,
    31620 AS code,
    -value AS value
FROM {{ ref('fact_liquidation_expenses') }}

UNION ALL

SELECT
    block_timestamp as ts,
    tx_hash as hash,
    21120 AS code,
    value AS value
FROM {{ ref('fact_liquidation_expenses') }}