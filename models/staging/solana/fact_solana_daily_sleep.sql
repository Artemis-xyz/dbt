{{
    config(
        materialized="incremental",
        unique_key=["from_address", "raw_date"],
        snowflake_warehouse="SOLANA",
    )
}}

{{ fact_daily_sleep("solana") }}
