{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="FRAX_SM",
    )
}}

{{
    fact_uniswap_v2_fork_dex_swaps(
        "0x43eC799eAdd63848443E2347C49f5f52e8Fe0F6f",
        "ethereum",
        (),
        "fraxswap",
        3000,
    )
}}
