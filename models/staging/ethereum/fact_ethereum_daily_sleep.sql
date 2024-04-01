{{
    config(
        materialized="incremental",
        unique_key=["from_address", "raw_date"],
        snowflake_warehouse="ETHEREUM",
    )
}}

{{ fact_daily_sleep("ethereum") }}
