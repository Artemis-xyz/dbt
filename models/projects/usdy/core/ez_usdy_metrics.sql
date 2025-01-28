{{
    config(
        materialized="table",
        snowflake_warehouse="USDY",
        database="usdy",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDY", breakdown='symbol') }}
