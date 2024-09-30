{{
    config( 
        materialized="table",
        snowflake_warehouse="PENDLE"
    )
}}

select
    date
    , chain
    , symbol
    , token_address
    , amount_usd
    , amount_native
from {{ref('fact_pendle_ethereum_tvl_by_token')}}
union all
select
    date
    , chain
    , symbol
    , token_address
    , amount_usd
    , amount_native
from {{ref('fact_pendle_arbitrum_tvl_by_token')}}