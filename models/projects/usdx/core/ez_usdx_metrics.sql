{{
    config(
        materialized="table",
        snowflake_warehouse="USDX",
        database="usdx",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDX", breakdown='symbol') }}
