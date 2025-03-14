{{
    config(
        materialized="table",
        snowflake_warehouse= "GHO",
        database="gho",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("GHO", breakdown='symbol') }}
