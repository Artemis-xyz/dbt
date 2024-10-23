{{
    config(
        materialized="table",
        snowflake_warehouse="metaplex",
        database="metaplex",
        schema="core",
        alias="ez_metrics",
    )
}}

