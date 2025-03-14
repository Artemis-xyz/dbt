{{
    config(
        materialized="table",
        snowflake_warehouse= "FLEXUSD",
        database="flexusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("FLEXUSD", breakdown='symbol') }}
