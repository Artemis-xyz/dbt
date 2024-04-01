{{ config(materialized="table", snowflake_warehouse="PANCAKESWAP_TVL_SM") }}

{{
    fact_daily_uniswap_v3_fork_tvl(
        "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865", "arbitrum", "pancakeswap_v3"
    )
}}
