{{
    config(
        materialized="table",
        snowflake_warehouse="CUSD",
        database="cusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("cUSD", breakdown='symbol') }}
