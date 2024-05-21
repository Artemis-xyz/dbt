{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="SUI",
    )
}}

{{ rolling_active_addresses("sui") }}