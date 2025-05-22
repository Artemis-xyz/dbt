{{
    config(
        materialized="table",
        snowflake_warehouse= "USD1",
        database="usd1",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USD1", breakdown='symbol') }}
