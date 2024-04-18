{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="ETHEREUM_XS",
    )
}}

{{ p2p_token_transfers("ethereum") }}