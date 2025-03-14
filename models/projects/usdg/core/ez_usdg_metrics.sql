{{
    config(
        materialized="table",
        snowflake_warehouse= "USDG",
        database="usdg",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDG", breakdown='symbol') }}
