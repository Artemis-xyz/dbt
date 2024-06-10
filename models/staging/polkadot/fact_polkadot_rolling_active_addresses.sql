{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="POLKADOT",
    )
}}

{{ rolling_active_addresses("polkadot") }}