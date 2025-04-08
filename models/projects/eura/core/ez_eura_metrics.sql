{{
    config(
        materialized="table",
        snowflake_warehouse="EURA",
        database="eura",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("EURA", breakdown='symbol') }}
