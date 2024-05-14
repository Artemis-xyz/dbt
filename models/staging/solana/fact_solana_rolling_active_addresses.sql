{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="SOLANA",
    )
}}

{{ rolling_active_addresses("solana") }}