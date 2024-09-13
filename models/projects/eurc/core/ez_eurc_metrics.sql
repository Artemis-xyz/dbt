{{
    config(
        materialized="table",
        snowflake_warehouse="EURC",
        database="eurc",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("EURC", breakdown='symbol') }}
