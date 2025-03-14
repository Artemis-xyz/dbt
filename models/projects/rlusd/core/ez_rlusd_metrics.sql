{{
    config(
        materialized="table",
        snowflake_warehouse= "RLUSD",
        database="rlusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("RLUSD", breakdown='symbol') }}
