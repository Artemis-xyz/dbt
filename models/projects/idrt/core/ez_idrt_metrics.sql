{{
    config(
        materialized="table",
        snowflake_warehouse= "IDRT",
        database="idrt",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("IDRT", breakdown='symbol') }}
