{{
    config(
        materialized="table",
        snowflake_warehouse="QUICKSWAP",
        database="quickswap",
        schema="raw",
        alias="ez_dex_swaps",
    )
}}

with
    dex_swaps as (
       select *
       from {{ ref("fact_quickswap_polygon_dex_swaps") }}
    )
select
    dex_swaps.block_timestamp,
    'quickswap' as app,
    'DeFi' as category,
    dex_swaps.chain,
    dex_swaps.version,
    dex_swaps.tx_hash,
    dex_swaps.sender,
    dex_swaps.recipient,
    dex_swaps.pool,
    dex_swaps.token_0,
    dex_swaps.token_0_symbol,
    dex_swaps.token_1,
    dex_swaps.token_1_symbol,
    dex_swaps.trading_volume,
    dex_swaps.trading_fees,
    dex_swaps.gas_cost_native,
from dex_swaps
where dex_swaps.block_timestamp::date < to_date(sysdate())