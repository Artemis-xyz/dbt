{{
    config(
        materialized="table",
        snowflake_warehouse="CEUR",
        database="ceur",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("cEUR", breakdown='symbol') }}
