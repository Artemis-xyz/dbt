{{
    config(
        materialized="table",
        snowflake_warehouse= "CGUSD",
        database="cgusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("cgUSD", breakdown='symbol') }}
