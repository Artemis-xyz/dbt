{{
    config(
        materialized = "table",
        snowflake_warehouse = "DYDX",
        database = "dydx_v4",
        schema = "raw",
        alias = "ez_price_performance"
    )
}}

with prices as (
    select 
        date_trunc('hour', block_timestamp) as hour
        , symbol
        , price
        , {{ is_nyc_operating_hours('hour') }} as nyc_operating_hours
    from {{ref('fact_dydx_v4_perps_prices')}} t1
    WHERE symbol in ('BTC', 'ETH')
)

select
    hour
    , symbol
    , max(price) as high
    , min(price) as low
    , avg(price) as average
    , median(price) as median
    , nyc_operating_hours
from prices
group by hour, symbol, nyc_operating_hours