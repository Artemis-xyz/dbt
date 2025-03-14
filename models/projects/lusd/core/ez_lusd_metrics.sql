{{
    config(
        materialized="table",
        snowflake_warehouse= "LUSD",
        database="lusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("LUSD", breakdown='symbol') }}
