{{
    config(
        materialized="table",
        snowflake_warehouse="CKES",
        database="ckes",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("cKES", breakdown='symbol') }}
