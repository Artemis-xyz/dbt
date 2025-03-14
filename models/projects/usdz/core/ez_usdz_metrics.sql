{{
    config(
        materialized="table",
        snowflake_warehouse= "USDZ",
        database="usdz",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDz", breakdown='symbol') }}
