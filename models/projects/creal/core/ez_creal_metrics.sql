{{
    config(
        materialized="table",
        snowflake_warehouse="CREAL",
        database="creal",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("cREAL", breakdown='symbol') }}
