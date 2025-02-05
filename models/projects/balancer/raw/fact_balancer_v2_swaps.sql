{{
    config(
        materialized = 'incremental',
        snowflake_warehouse = 'MEDIUM',
        database = 'BALANCER',
        schema = 'raw',
        alias = 'fact_balancer_v2_swaps'
    )
}}

with unioned_swaps as (
{{
    dbt_utils.union_relations(
        relations = [
            ref('fact_balancer_v2_arbitrum_swaps'),
            ref('fact_balancer_v2_polygon_swaps'),
            ref('fact_balancer_v2_ethereum_swaps')
        ]
    )
}}
)

select
    block_timestamp,
    chain,
    'balancer' as app,
    'v2' as version,

    -- Transaction information
    tx_hash,
    from_address as sender,
    to_address as recipient,

    -- Pool information
    pool_address,
    pool_id,

    -- Input token information
    tokens_in_pool,
    amount_in_native,
    amount_in_usd,
    amount_in_fee_native,
    amount_in_fee_usd,
    token_in_symbol,

    -- Output token information
    amount_out_native,
    amount_out_usd,
    amount_out_fee_native,
    amount_out_fee_usd,
    token_out_symbol,

    -- Fee information
    swap_fee_pct,
    fee_token,
    fee_native,
    fee_usd
from unioned_swaps