{{ config(materialized="table", snowflake_warehouse="PANCAKESWAP_SM") }}

{{
    fact_daily_uniswap_v3_fork_tvl_by_pool(
        "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865", "ethereum", "pancakeswap"
    )
}}
