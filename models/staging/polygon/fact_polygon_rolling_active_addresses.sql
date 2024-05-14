{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="POLYGON",
    )
}}

{{ rolling_active_addresses("polygon") }}