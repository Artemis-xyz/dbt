{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="TON",
        database="ton",
        schema="raw",
        alias="ez_p2p_stablecoin_transfers",
    )
}}

{{ p2p_stablecoin_transfers("ton") }}