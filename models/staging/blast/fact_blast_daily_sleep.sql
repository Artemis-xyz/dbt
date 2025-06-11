{{
    config(
        materialized="incremental",
        unique_key=["from_address", "raw_date"],
        enabled=false
    )
}}

{{ fact_daily_sleep("blast") }}
