{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="POLYGON_SM",
    )
}}

{{ p2p_token_transfers("polygon") }}