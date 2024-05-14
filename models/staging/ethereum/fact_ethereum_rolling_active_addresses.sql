{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="ETHEREUM",
    )
}}

{{ rolling_active_addresses("ethereum") }}