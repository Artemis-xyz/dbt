{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_swaps'
    )
}}

with combined_swaps as (
    SELECT * FROM {{ ref('fact_aerodrome_v1_swaps') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_aerodrome_v2_swaps') }}
)
SELECT
    block_timestamp,
    app,
    category,
    chain,
    version,
    tx_hash,
    sender,
    recipient,
    pool_address,
    token_in_address,
    token_out_address,
    amount_in_native,
    amount_in_usd,
    amount_out_native,
    amount_out_usd,
    swap_fee_pct,
    fee_usd,
    revenue,
    supply_side_revenue_usd
FROM combined_swaps