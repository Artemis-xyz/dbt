{{
    config(
        materialized="table",
        snowflake_warehouse= "LISUSD",
        database="lisusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("LISUSD", breakdown='symbol') }}
