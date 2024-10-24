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
            when date_part('DOW', convert_timezone('UTC', 'America/New_York', block_timestamp)) IN (0, 6) then 'FALSE'
            when convert_timezone('UTC', 'America/New_York', block_timestamp)::time between '09:00:00' and '16:59:59' then 'TRUE'
            else 'FALSE'
        END AS nyc_operating_hours
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