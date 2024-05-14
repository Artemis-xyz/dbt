{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="BASE",
    )
}}

{{ rolling_active_addresses("base") }}