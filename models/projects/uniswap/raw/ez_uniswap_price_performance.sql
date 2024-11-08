{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="raw",
        alias="ez_price_performance",
    )
}}


with
tracked_metadata as (
    select *
    from {{ref('wrapped_token_majors_by_chain')}}
),
prices as (
    select 
        date_trunc('hour', block_timestamp) as hour
        , unwrapped_symbol as symbol
        , price
        , {{ is_nyc_operating_hours(hour) }} as nyc_operating_hours
    from {{ref('fact_uniswap_dex_token_prices')}} t1
    inner join tracked_metadata 
        on lower(t1.token_address) = lower(tracked_metadata.contract_address) 
        and t1.chain = tracked_metadata.chain
    -- Filter out EURC pairs becuase we want to price things in USD
    where pair not like '%EURC%'
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