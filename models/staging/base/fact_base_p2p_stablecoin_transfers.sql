{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="BASE",
    )
}}

{{ p2p_stablecoin_transfers("base") }}