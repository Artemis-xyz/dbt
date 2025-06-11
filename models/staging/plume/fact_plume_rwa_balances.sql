{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        snowflake_warehouse="RWA",
    )
}}

{{rwa_balances("plume")}}