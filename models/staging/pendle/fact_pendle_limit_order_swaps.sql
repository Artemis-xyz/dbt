{{
    config(
        materialized="table",
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
    origin_from_address,
    tx_hash,
    event_index,
    order_type,
    yt_address,
    pt_address,
    token_address,
    sy_address,
    symbol,
    volume_native,
    fee_native,
    volume,
    fee,
    net_input_from_maker_native,
    net_output_to_maker_native,
    maker,
    taker
from limit_orders