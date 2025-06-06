{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
    )
}}

SELECT
    chain,
    block_timestamp,
    tx_hash,
    event_index,
    block_timestamp::date as date,
    'AMM' as order_type,
    market_address,
    yt_address,
    pt_address,
    sy_address,
    token_address,
    symbol as token,
    net_sy_out_native as volume_native,
    net_sy_out_usd as volume,
    total_fees_native as fees_native,
    total_fees_usd as fees,
    supply_side_fees_native,
    supply_side_fees_usd as supply_side_fees,
    revenue_native,
    revenue_usd as revenue,
    NULL as maker,
    NULL as taker
FROM {{ ref('fact_pendle_amm_swaps') }}
UNION ALL
SELECT
    chain,
    block_timestamp,
    tx_hash,
    event_index,
    block_timestamp::date as date,
    'LIMIT_ORDER' as order_type,
    NULL as market_address,
    yt_address,
    pt_address,
    sy_address,
    token_address,
    symbol as token,
    volume_native,
    volume,
    fee_native as fees_native,
    fee as fees,
    NULL as supply_side_fees_native,
    NULL as supply_side_fees,
    NULL as revenue_native,
    NULL as revenue,
    maker,
    taker
FROM {{ ref('fact_pendle_limit_order_swaps') }}
