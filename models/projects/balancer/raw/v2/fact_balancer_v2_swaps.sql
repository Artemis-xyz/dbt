{{
    config(
        materialized = 'table',
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
            ref('fact_balancer_v2_ethereum_swaps'),
            ref('fact_balancer_v2_gnosis_swaps')
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

    -- Input token information
    amount_in_native,
    amount_in_usd,
    token_in_symbol,
    token_in_address,

    -- Output token information
    amount_out_native,
    amount_out_usd,
    token_out_symbol,
    token_out_address,

    -- Fee information
    swap_fee_pct,
    fee_token,
    fee_native,
    fee_usd,
    CASE WHEN block_timestamp::date > '2022-02-16'
        then 0.5
    when block_timestamp::date > '2021-12-13'
        then 0.1
    end as protocol_fee_pct,
    fee_usd * protocol_fee_pct as revenue,
    fee_native * protocol_fee_pct as revenue_native,
    fee_usd * (1 - protocol_fee_pct) as supply_side_revenue_usd,
    fee_native * (1 - protocol_fee_pct) as supply_side_revenue_native
from unioned_swaps