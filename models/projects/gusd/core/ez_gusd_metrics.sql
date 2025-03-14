{{
    config(
        materialized="table",
        snowflake_warehouse= "GUSD",
        database="gusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("GUSD", breakdown='symbol') }}
