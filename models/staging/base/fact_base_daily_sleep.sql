{{
    config(
        materialized="incremental",
        unique_key=["from_address", "raw_date"],
        snowflake_warehouse="BASE",
    )
}}

{{ fact_daily_sleep("base") }}
