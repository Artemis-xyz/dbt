{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
    )
}}
{{
    fact_uniswap_v3_fork_swap_events(
        "0x740b1c1de25031C31FF4fC9A62f554A55cdC1baD", "avalanche", "uniswap", "v3"
    )
}}
