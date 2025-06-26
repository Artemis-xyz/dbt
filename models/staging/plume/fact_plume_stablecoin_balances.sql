{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        snowflake_warehouse="RWA",
    )
}}

{{stablecoin_balances_from_addresses("plume")}}