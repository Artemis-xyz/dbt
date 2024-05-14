{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="ARBITRUM",
    )
}}

{{ rolling_active_addresses("arbitrum") }}