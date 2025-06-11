{{
    config(
        materialized="table",
        snowflake_warehouse= "BRLA",
        database="brla",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("BRLA", breakdown='symbol') }}
