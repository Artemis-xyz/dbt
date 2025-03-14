{{
    config(
        materialized="table",
        snowflake_warehouse= "BOLD",
        database="bold",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("BOLD", breakdown='symbol') }}
