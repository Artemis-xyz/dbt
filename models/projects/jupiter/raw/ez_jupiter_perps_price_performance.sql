{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER",
        database="jupiter",
        schema="raw",
        alias="ez_perps_price_performance",
    )
}}

with agg as (
    SELECT
        date_trunc('hour', agg.block_timestamp) as hour,
        agg.price,
        agg.mint,
        m.symbol,
        CASE 
        , {{ is_nyc_operating_hours(hour) }} as nyc_operating_hours
    FROM {{ ref('fact_jupiter_perps_txs') }} agg
    LEFT JOIN solana_flipside.price.ez_asset_metadata m ON m.token_address = agg.mint
)

SELECT
    hour
    , symbol
    , mint
    , max(price) as high
    , min(price) as low
    , avg(price) as average
    , median(price) as median
    , nyc_operating_hours
FROM agg
GROUP BY hour, symbol, mint, nyc_operating_hours