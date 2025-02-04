{{
    config(
        materialized="table",
        snowflake_warehouse= "FRAX_USD",
        database="frax_usd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("FRAX", breakdown='symbol') }}