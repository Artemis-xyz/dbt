{{
    config(
        materialized="table",
        snowflake_warehouse= "MIM",
        database="mim",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("MIM", breakdown='symbol') }}
