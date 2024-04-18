{{
    config(
        materialized="incremental",
        unique_key=["from_address", "raw_date"],
        snowflake_warehouse="BLAST",
    )
}}

{{ fact_daily_sleep("blast") }}
