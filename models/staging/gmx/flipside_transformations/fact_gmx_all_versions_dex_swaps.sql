{{ config(materialized="table", snowflake_warehouse="GMX") }}

with all_dex_swaps as (
    select
        date,
        chain,
        version,
        block_timestamp,
        tx_hash,
        tokenIn,
        amountIn_usd as volume,
        amount_fees_usd as fees,
        sender as trader
    from {{ref('fact_gmx_v1_dex_swaps')}}
    union all
    select
        date,
        chain,
        version,
        block_timestamp,
        tx_hash,
        tokenIn,
        amountIn_usd as volume,
        amount_fees_usd as fees,
        sender as trader
    from {{ref('fact_gmx_v2_dex_swaps')}}
) select * from all_dex_swaps