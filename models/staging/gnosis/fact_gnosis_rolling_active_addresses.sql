{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="GNOSIS",
    )
}}

{{ rolling_active_addresses("gnosis") }}
