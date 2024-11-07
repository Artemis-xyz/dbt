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
        , CASE 
            when date_part('DOW', convert_timezone('UTC', 'America/New_York', block_timestamp)) IN (0, 6) then 'FALSE'
            when convert_timezone('UTC', 'America/New_York', block_timestamp)::time between '09:00:00' and '15:59:59' then 'TRUE'
            else 'FALSE'
        END AS nyc_operating_hours
    from {{ref('fact_dydx_v4_perps_prices')}} t1
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