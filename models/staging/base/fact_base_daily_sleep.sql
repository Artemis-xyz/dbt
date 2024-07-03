{{
    config(
        materialized="incremental",
        unique_key=["from_address", "raw_date"],
    )
}}

{{ fact_daily_sleep("base") }}
