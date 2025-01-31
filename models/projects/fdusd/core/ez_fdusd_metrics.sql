{{
    config(
        materialized="table",
        snowflake_warehouse="FDUSD",
        database="fdusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("FDUSD", breakdown='symbol') }}
