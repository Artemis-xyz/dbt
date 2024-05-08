{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="ARBITRUM",
    )
}}

{{ p2p_stablecoin_transfers("arbitrum") }}