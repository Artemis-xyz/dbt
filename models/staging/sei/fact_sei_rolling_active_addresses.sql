{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="SEI",
    )
}}

{{ rolling_active_addresses("sei", "_v2") }}
