{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_gho_treasury_revenue",
    )
}}

with
event_logs as (
    select 
        block_timestamp
        , '0x' || substr(topics[2]::string, 27, 40) as asset
        , pc_dbt_db.prod.hex_to_int(data) as amount
    from ethereum_flipside.core.fact_event_logs 
    where contract_address = lower('0x00907f9921424583e7ffBfEdf84F92B7B2Be4977')
        and topics[0]::string = '0xb29fcda740927812f5a71075b62e132bead3769a455319c29b9a1cc461a65475'
)
, priced_logs as (
    select
        block_timestamp::date as date
        , asset
        , amount / pow(10, decimals) as amount_nominal
        , amount_nominal * price as amount_usd
    from event_logs
    left join ethereum_flipside.price.ez_prices_hourly
        on date_trunc(hour, block_timestamp) = hour
        and lower(asset) = lower(token_address)
)
select
    date
    , 'AAVE GHO' as protocol
    , 'ethereum' as chain
    , asset as token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from priced_logs
group by 1, 4
order by 1 desc