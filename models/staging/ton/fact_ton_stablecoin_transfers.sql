{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
    )
}}
{{agg_chain_stablecoin_transfers("ton")}}