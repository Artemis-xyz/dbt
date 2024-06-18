{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}

{{
    fact_uniswap_v2_fork_dex_swaps(
        "0x1097053Fd2ea711dad45caCcc45EfF7548fCB362",
        "ethereum",
        (),
        "pancakeswap",
        2500,
        version="v2"
    )
}}
