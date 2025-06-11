{{
    config(
        materialized="table",
        snowflake_warehouse= "IDRX",
        database="idrx",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("IDRX", breakdown='symbol') }}
