{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="SUSHISWAP_SM",
    )
}}

{{
    fact_uniswap_v2_fork_dex_swaps(
        "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac",
        "ethereum",
        (),
        "sushiswap",
        3000,
        version="v2"
    )
}}
