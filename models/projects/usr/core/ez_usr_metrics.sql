{{
    config(
        materialized="table",
        snowflake_warehouse= "USR",
        database="usr",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USR", breakdown='symbol') }}