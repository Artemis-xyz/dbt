{{
    config(
        materialized="incremental",
        unique_key=["from_address", "raw_date"],
        snowflake_warehouse="OPTIMISM",
    )
}}

{{ fact_daily_sleep("optimism") }}
