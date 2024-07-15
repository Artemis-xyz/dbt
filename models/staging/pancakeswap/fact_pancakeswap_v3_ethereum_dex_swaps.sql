{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
    )
}}

{{
    fact_uniswap_v3_fork_dex_swaps(
        "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "ethereum",
        (),
        "pancakeswap",
        "v3",
    )
}}
