{{
    config(
        materialized="table",
        snowflake_warehouse= "USD3",
        database="usd3",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USD3", breakdown='symbol') }}
