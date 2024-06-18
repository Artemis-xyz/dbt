{{
    config(
        materialized="table",
        snowflake_warehouse="CURVE_SM",
        database="curve",
        schema="raw",
        alias="ez_dex_swaps",
    )
}}

with
    dex_swaps as (
       {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_curve_arbitrum_dex_swaps"),
                    ref("fact_curve_avalanche_dex_swaps"),
                    ref("fact_curve_optimism_dex_swaps"),
                    ref("fact_curve_ethereum_dex_swaps"),
                    ref("fact_curve_polygon_dex_swaps"),
                ],
            )
        }}
    )
select
    dex_swaps.block_timestamp,
    'curve' as app,
    'DeFi' as category,
    dex_swaps.chain,
    dex_swaps.tx_hash,
    dex_swaps.sender,
    dex_swaps.recipient,
    dex_swaps.pool,
    dex_swaps.token_out,
    dex_swaps.token_out_symbol,
    dex_swaps.token_in,
    dex_swaps.token_in_symbol,
    dex_swaps.trading_volume,
    dex_swaps.trading_fees,
    dex_swaps.trading_revenue,
    dex_swaps.gas_cost_native
from dex_swaps
where dex_swaps.block_timestamp::date < to_date(sysdate())