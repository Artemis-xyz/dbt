{{
    config(
        materialized="table",
        snowflake_warehouse="EXACTLY",
        database="exactly",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

select
    date::date as date,
    token,
    sum(transfer_volume) as transfer_volume
from {{ ref("fact_exactly_transfers") }}
group by 1, 2