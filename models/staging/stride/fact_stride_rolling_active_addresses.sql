{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="STRIDE",
    )
}}

{{ rolling_active_addresses("stride") }}