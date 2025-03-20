{{
    config(
        materialized="table",
        snowflake_warehouse="EURS",
        database="eurs",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("EURS", breakdown='symbol') }}
