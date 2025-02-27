{{
    config(
        materialized="table",
        snowflake_warehouse= "SUSD",
        database="susd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("SUSD", breakdown='symbol') }}