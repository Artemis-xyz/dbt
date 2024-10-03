{{
    config(
        materialized="table",
        snowflake_warehouse="AUSD",
        database="ausd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("AUSD", breakdown='symbol') }}
