{{
    config(
        materialized="table",
        snowflake_warehouse="EXACTLY",
        database="exactly",
        schema="core",
        alias="ez_metrics",
    )
}}

select
    date::date as date,
    sum(transfer_volume) as transfer_volume
from {{ ref("fact_exactly_transfers") }}
group by 1