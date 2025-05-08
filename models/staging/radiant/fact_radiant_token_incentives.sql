{{config(materialized="table", snowflake_warehouse='RADIANT')}}

with ethereum_data as (
    select date, chain, amount_native, amount_usd from {{ ref("fact_radiant_ethereum_token_incentives") }}
)
, arbitrum_data as (
    select date, chain, amount_native, amount_usd from {{ ref("fact_radiant_arbitrum_token_incentives") }}
)
, bsc_data as (
    select date, chain, amount_native, amount_usd from {{ ref("fact_radiant_bsc_token_incentives") }}
)
, all_data as (
    select date, chain, amount_native, amount_usd from ethereum_data
        union all
    select date, chain, amount_native, amount_usd from arbitrum_data
        union all
    select date, chain, amount_native, amount_usd from bsc_data
)
select 
    date
    , chain
    , amount_native
    , amount_usd
from all_data
