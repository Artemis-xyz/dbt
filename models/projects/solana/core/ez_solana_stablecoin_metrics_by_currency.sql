{{
    config(
        materialized="table",
        database="solana",
        schema="core",
        alias="ez_stablecoin_metrics_by_currency",
        snowflake_warehouse="solana",
    )
}}

select *
from {{ ref("agg_solana_stablecoin_metrics") }}
