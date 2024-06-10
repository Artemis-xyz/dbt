{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="FANTOM",
    )
}}

{{ rolling_active_addresses("fantom") }}