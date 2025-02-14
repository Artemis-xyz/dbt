{{
    config(
        materialized="table",
        snowflake_warehouse="USD0",
        database="usd0",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USD0", breakdown='symbol') }}
