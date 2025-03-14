{{
    config(
        materialized="table",
        snowflake_warehouse= "USDTB",
        database="usdtb",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDtb", breakdown='symbol') }}
