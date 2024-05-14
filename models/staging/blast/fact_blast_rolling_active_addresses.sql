{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="BLAST",
    )
}}

{{ rolling_active_addresses("blast") }}