{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="AVALANCHE",
    )
}}

{{ rolling_active_addresses("avalanche") }}