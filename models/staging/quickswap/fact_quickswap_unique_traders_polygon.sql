{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="DEX_SM",
    )
}}

{{
    fact_daily_uniswap_fork_unique_traders(
        "0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32",
        "polygon",
        "PairCreated",
        "pair",
        "quickswap",
    )
}}
