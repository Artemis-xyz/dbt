{{ config(materialized="table") }}

{{
    fact_daily_uniswap_v2_fork_tvl_by_pool(
        "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac", "ethereum", "sushiswap"
    )
}}
