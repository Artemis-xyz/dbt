{{
    config(
        materialized="table",
        snowflake_warehouse= "USN",
        database="usn",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USN", breakdown='symbol') }}
