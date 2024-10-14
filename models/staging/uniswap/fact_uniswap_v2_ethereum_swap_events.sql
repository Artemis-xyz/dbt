{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
    )
}}

{{
    fact_uniswap_v2_fork_swap_events(
        "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
        "ethereum",
    )
}}
