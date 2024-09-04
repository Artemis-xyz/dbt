{{
    config(
        materialized="table",
        snowflake_warehouse="USDT",
        database="usdt",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDT", breakdown='symbol') }}
