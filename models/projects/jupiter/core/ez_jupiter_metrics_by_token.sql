{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER",
        database="jupiter",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

-- TODO: add aggregator metrics, dca metrics, limit order metrics

SELECT
    block_timestamp::date as date,
    chain,
    app,
    symbol as token,
    mint as token_address,

    -- Standardized Metrics
    , trading_fees as perp_fees
    , trading_volume as perp_volume
    , unique_traders as perp_dau
    
FROM {{ ref('fact_jupiter_perps_txs') }} t
LEFT JOIN {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_asset_metadata') }} m ON m.token_address = t.mint
GROUP BY 1,2,3,4,5