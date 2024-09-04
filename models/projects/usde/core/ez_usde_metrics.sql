{{
    config(
        materialized="table",
        snowflake_warehouse="USDE",
        database="usde",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDe", breakdown='symbol') }}
