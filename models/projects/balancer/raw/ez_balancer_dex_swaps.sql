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
    {{
        dbt_utils.union_relations(
            relations = [
                ref('fact_balancer_v1_swaps'),
                ref('fact_balancer_v2_swaps')
            ],
            column_override = {
                "REVENUE": "float"
            }
        )
    }}
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
    case 
        when amount_in_usd < 1e8
            then amount_in_usd
        when amount_out_usd < 1e8
            then amount_out_usd
        else NULL
    end as trading_volume,
    case 
        when amount_in_usd < 1e8
            then amount_in_native
        when amount_out_usd < 1e8
            then amount_out_native
        else NULL
    end as trading_volume_native,
    swap_fee_pct,
    case 
        when fee_usd < 1e6
            then fee_usd
        else NULL
    end as fee_usd,
    case 
        when fee_usd < 1e6
            then fee_native
        else NULL
    end as fee_native,
    case 
        when revenue < 1e5
            then revenue
        else NULL
    end as revenue,
    case 
        when revenue < 1e5
            then revenue_native
        else NULL
    end as revenue_native,
    case 
        when supply_side_revenue_usd < 1e5
            then supply_side_revenue_usd
        else NULL
    end as supply_side_revenue_usd,
    case 
        when supply_side_revenue_usd < 1e5
            then supply_side_revenue_native
        else NULL
    end as supply_side_revenue_native
from all_swaps