{{
    config(
        materialized="table",
        snowflake_warehouse= "CRVUSD",
        database="crvusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("crvUSD", breakdown='symbol') }}
