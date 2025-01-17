{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
    )
}}
{{
    fact_uniswap_v3_fork_swap_events(
        "0x33128a8fC17869897dcE68Ed026d694621f6FDfD", "base", "uniswap", "v3"
    )
}}
