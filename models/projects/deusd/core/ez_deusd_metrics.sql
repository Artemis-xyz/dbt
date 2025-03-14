{{
    config(
        materialized="table",
        snowflake_warehouse= "DEUSD",
        database="deusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("DEUSD", breakdown='symbol') }}
