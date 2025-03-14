{{
    config(
        materialized="table",
        snowflake_warehouse= "ANGLE_USD",
        database="angle_usd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("ANGLE_USD", breakdown='symbol') }}
