{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="ZKSYNC",
    )
}}

{{ rolling_active_addresses("zksync") }}