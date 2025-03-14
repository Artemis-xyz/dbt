{{
    config(
        materialized="table",
        snowflake_warehouse= "FXUSD",
        database="fxusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("fxUSD", breakdown='symbol') }}
