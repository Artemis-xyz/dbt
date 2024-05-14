{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="TRON",
    )
}}

{{ rolling_active_addresses("tron") }}