{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="OPTIMISM",
    )
}}

{{ rolling_active_addresses("optimism") }}