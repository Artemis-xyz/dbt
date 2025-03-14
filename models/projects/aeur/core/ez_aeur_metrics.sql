{{
    config(
        materialized="table",
        snowflake_warehouse= "AEUR",
        database="aeur",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("AEUR", breakdown='symbol') }}
