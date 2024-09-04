{{
    config(
        materialized="table",
        snowflake_warehouse="DAI",
        database="dai",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("DAI", breakdown='symbol') }}
