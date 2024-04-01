{{
    config(
        materialized="table",
        database="tron",
        schema="core",
        alias="ez_stablecoin_metrics_by_currency",
        snowflake_warehouse="TRON",
    )
}}

select *
from {{ ref("agg_tron_stablecoin_metrics") }}
