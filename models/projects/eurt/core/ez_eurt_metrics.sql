{{
    config(
        materialized="table",
        snowflake_warehouse= "EURT",
        database="eurt",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("EURT", breakdown='symbol') }}
