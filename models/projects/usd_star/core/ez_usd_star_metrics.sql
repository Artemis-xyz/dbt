{{
    config(
        materialized="table",
        snowflake_warehouse="USD_STAR",
        database="usd_star",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USD*", breakdown='symbol') }}
