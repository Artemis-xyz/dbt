{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
        alias='ez_dex_swaps'
    )
}}

with v2_swaps as (
    select * from ref("fact_balancer_v2_swaps")
)
, v1_swaps as (
    select * from ref("fact_balancer_v1_swaps")
)

unioned_swaps as (
    select * from v2_swaps
    union all
    select * from v1_swaps
)

select
    block_timestamp,
    app,
    'DeFi' as category,
    chain,
    version,
    tx_hash,
    sender,
    recipient,
    pool,
    token_0,
    token_0_symbol,
    token_1,
    token_1_symbol,
    trading_volume,
    trading_fees,
    gas_cost_native
from balancer_swaps