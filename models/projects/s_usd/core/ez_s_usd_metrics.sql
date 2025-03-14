{{
    config(
        materialized="table",
        snowflake_warehouse= "S_USD",
        database="s_usd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("S_USD", breakdown='symbol') }}
