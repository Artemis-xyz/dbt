{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER",
        database="jupiter",
        schema="core",
        alias="ez_metrics_by_market",
    )
}}

SELECT
    block_timestamp::date as date,
    chain,
    app,
    symbol,
    mint as token_address,
    sum(fee_usd) as trading_fees,
    sum(size_usd) as trading_volume,
    count(distinct owner) as unique_traders
FROM {{ ref('fact_jupiter_perps_txs') }} t
LEFT JOIN {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_asset_metadata') }} m ON m.token_address = t.mint
GROUP BY 1,2,3,4,5