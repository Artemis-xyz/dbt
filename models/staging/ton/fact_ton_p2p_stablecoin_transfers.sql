{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
    )
}}
{{ p2p_stablecoin_transfers("ton") }}