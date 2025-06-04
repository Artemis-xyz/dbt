{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
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
    chain,
    block_timestamp,
    transaction_hash,
    event_index,
    order_type,
    yt_address,
    token_address,
    symbol,
    volume_native,
    fee_native,
    volume_usd,
    fee_usd,
    net_input_from_maker,
    net_output_to_maker,
    maker,
    taker
from limit_orders