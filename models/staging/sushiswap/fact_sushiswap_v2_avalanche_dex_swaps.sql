{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="SUSHISWAP_SM",
    )
}}

{{
    fact_uniswap_v2_fork_dex_swaps(
        "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
        "avalanche",
        (),
        "sushiswap",
        3000,
        version="v2"
    )
}}
