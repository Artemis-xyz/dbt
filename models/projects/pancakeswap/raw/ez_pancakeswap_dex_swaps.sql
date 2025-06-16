{{
    config(
        materialized="table",
        snowflake_warehouse="PANCAKESWAP_SM",
        database="pancakeswap",
        schema="raw",
        alias="ez_dex_swaps",
    )
}}

with
    dex_swaps as (
       {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_pancakeswap_v2_arbitrum_dex_swaps"),
                    ref("fact_pancakeswap_v2_base_dex_swaps"),
                    ref("fact_pancakeswap_v2_bsc_dex_swaps"),
                    ref("fact_pancakeswap_v2_ethereum_dex_swaps"),
                    ref("fact_pancakeswap_v3_arbitrum_dex_swaps"),
                    ref("fact_pancakeswap_v3_base_dex_swaps"),
                    ref("fact_pancakeswap_v3_bsc_dex_swaps"),
                    ref("fact_pancakeswap_v3_ethereum_dex_swaps"),
                ],
            )
        }}
    )
select
    dex_swaps.block_timestamp,
    'pancakeswap' as app,
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
    dex_swaps.fee_percent
from dex_swaps
where dex_swaps.block_timestamp::date < to_date(sysdate())