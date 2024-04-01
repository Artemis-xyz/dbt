{{
    config(
        materialized="incremental",
        unique_key=["from_address", "raw_date"],
        snowflake_warehouse="polygon",
    )
}}

{{ fact_daily_sleep("polygon") }}
