{{
    config(
        materialized="table",
        snowflake_warehouse= "LVLUSD",
        database="lvlusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("LVLUSD", breakdown='symbol') }}
