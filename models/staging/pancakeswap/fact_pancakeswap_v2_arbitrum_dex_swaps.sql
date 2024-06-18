{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}

{{
    fact_uniswap_v2_fork_dex_swaps(
        "0x02a84c1b3BBD7401a5f7fa98a384EBC70bB5749E",
        "arbitrum",
        (),
        "pancakeswap",
        2500,
        version="v2"
    )
}}
