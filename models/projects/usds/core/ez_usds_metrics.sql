{{
    config(
        materialized="table",
        snowflake_warehouse= "USDS",
        database="usds",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDS", breakdown='symbol') }}
