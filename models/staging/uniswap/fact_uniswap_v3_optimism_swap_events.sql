{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
    )
}}
{{
    fact_uniswap_v3_fork_swap_events(
        "0x1F98431c8aD98523631AE4a59f267346ea31F984", "optimism", "uniswap", "v3"
    )
}}
