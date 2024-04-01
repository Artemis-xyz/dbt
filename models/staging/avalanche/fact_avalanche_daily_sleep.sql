{{
    config(
        materialized="incremental",
        unique_key=["from_address", "raw_date"],
        snowflake_warehouse="AVALANCHE",
    )
}}

{{ fact_daily_sleep("avalanche") }}
