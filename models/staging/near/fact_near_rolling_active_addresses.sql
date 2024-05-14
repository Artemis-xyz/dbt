{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="NEAR",
    )
}}

{{ rolling_active_addresses("near") }}