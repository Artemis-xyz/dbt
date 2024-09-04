{{
    config(
        materialized="table",
        snowflake_warehouse="BUSD",
        database="busd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("BUSD", breakdown='symbol') }}
