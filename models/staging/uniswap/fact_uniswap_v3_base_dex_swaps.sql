{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="UNISWAP_SM",
    )
}}
{{
    fact_uniswap_v3_fork_dex_swaps(
        "0x33128a8fC17869897dcE68Ed026d694621f6FDfD", "base", (), "uniswap", "v3"
    )
}}
