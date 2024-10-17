{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
    )
}}

{{fact_dex_priced_tokens('uniswap', 'v2', 'ethereum')}}
