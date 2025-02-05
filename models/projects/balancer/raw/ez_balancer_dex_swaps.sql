{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
        alias='ez_dex_swaps'
    )
}}

with all_swaps as (
    dbt_utils.union_relations(
        relations = [
            ref('fact_balancer_v1_swaps'),
            ref('fact_balancer_v2_swaps')
        ]
    )
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
    pool_address,
    token_in_address,
    token_in_symbol,
    token_out_address,
    token_out_symbol,
    amount_in_native,
    amount_in_usd,
    amount_out_native,
    amount_out_usd,
    swap_fee_pct,
    fee_usd,
    revenue,
    supply_side_revenue_usd
from unioned_swaps