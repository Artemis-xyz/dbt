{{
    config(
        materialized="table",
        snowflake_warehouse= "DOLA",
        database="dola",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("DOLA", breakdown='symbol') }}
