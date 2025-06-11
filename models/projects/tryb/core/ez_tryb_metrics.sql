{{
    config(
        materialized="table",
        snowflake_warehouse= "TRYB",
        database="tryb",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("TRYB", breakdown='symbol') }}
