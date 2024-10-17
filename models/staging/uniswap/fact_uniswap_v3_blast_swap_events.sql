{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
    )
}}
{{
    fact_uniswap_v3_fork_swap_events(
        "0x792edAdE80af5fC680d96a2eD80A44247D2Cf6Fd", "blast", "uniswap", "v3"
    )
}}
