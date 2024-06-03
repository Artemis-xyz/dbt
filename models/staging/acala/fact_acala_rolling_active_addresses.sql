{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="ACALA",
    )
}}

{{ rolling_active_addresses("acala") }}