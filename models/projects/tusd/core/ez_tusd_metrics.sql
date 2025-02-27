{{
    config(
        materialized="table",
        snowflake_warehouse= "TUSD",
        database="tusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("TUSD", breakdown='symbol') }}