{{
    config(
        materialized="incremental",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

with limit_orders as (
    select * from {{ref('fact_pendle_arbitrum_limit_orders')}}
    union all
    select * from {{ref('fact_pendle_base_limit_orders')}}
    union all
    select * from {{ref('fact_pendle_bsc_limit_orders')}}
    union all
    select * from {{ref('fact_pendle_ethereum_limit_orders')}}
)
select
    block_timestamp
from limit_orders