{{ 
    config(
        materialized="incremental",
        unique_key=["to_address", "tx_hash", "index"],
        snowflake_warehouse="ETHEREUM",
    )
}}

{{ wallet_seeder_funding_recipients("ethereum") }}
