{{
    config(
        materialized="table",
        snowflake_warehouse= "FRXUSD",
        database="frxusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("FRXUSD", breakdown='symbol') }}
