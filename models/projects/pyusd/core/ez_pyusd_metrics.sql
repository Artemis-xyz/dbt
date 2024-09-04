{{
    config(
        materialized="table",
        snowflake_warehouse="PYUSD",
        database="pyusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("PYUSD", breakdown='symbol') }}
