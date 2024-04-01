{{
    config(
        materialized="incremental",
        unique_key=["from_address", "raw_date"],
        snowflake_warehouse="ARBITRUM",
    )
}}

{{ fact_daily_sleep("arbitrum") }}
