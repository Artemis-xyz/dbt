{{
    config(
        materialized="table",
        snowflake_warehouse= "BUIDL",
        database="buidl",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("BUIDL", breakdown='symbol') }}
