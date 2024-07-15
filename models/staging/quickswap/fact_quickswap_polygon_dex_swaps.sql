{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
    )
}}
{{
    fact_uniswap_v2_fork_dex_swaps(
        "0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32",
        "polygon",
        (),
        "quickswap",
        3000,
    )
}}
