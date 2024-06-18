{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}

{{
    fact_uniswap_v3_fork_dex_swaps(
        "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "bsc",
        (
            "0x68f801cc634a9d2eb8ee336ac1d43f7f17967a15",
            "0x41ca6725cd58d49eae58d7e5a304907bde0caa19",
        ),
        "pancakeswap",
        "v3",
    )
}}
