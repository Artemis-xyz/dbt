{{
    config(
        materialized="table",
        snowflake_warehouse="GMX",
        database="gmx",
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
        , CASE 
            when date_part('DOW', convert_timezone('UTC', 'America/New_York', block_timestamp)) IN (0, 6) then 'FALSE'
            when convert_timezone('UTC', 'America/New_York', block_timestamp)::time between '09:00:00' and '15:59:59' then 'TRUE'
            else 'FALSE'
        END AS nyc_operating_hours
    from {{ref('fact_gmx_all_versions_trades')}} t1
    inner join tracked_metadata 
        on lower(t1.token_address) = lower(tracked_metadata.contract_address) 
        and t1.chain = tracked_metadata.chain
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