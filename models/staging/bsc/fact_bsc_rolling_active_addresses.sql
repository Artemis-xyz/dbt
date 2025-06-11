{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="BSC_MD",
    )
}}

{{ rolling_active_addresses("bsc", "_v2") }}