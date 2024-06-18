{{
    config(
        materialized="table",
        snowflake_warehouse="TRADER_JOE",
        database="trader_joe",
        schema="raw",
        alias="ez_dex_swaps",
    )
}}

with
    dex_swaps as (
       {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_trader_joe_arbitrum_dex_swaps"),
                    ref("fact_trader_joe_avalanche_dex_swaps"),
                ],
            )
        }}
    )
select
    dex_swaps.block_timestamp,
    'trader_joe' as app,
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