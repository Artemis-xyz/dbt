{{
    config(
        materialized="table",
        snowflake_warehouse="EXACTLY",
        database="exactly",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select
    date::date as date,
    'optimism' as chain,
    sum(transfer_volume) as transfer_volume
from {{ ref("fact_exactly_transfers") }}
group by 1