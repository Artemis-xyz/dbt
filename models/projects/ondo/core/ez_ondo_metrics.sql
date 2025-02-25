{{
    config(
        materialized="table",
        database = 'ondo',
        schema = 'core',
        snowflake_warehouse = 'ONDO',
        alias = 'ez_metrics'
    )
}}

with date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between '2023-01-26' and to_date(sysdate())
)

, fees as (
    select date, fee as fees from {{ ref("fact_ondo_ousg_fees") }}
)
, tvl as (
    select
        date,
        sum(tokenized_mcap_change) as tokenized_mcap_change,
        sum(tokenized_mcap) as tokenized_mcap,
    from {{ ref("ez_ondo_metrics_by_chain") }}
    group by 1
)

select
    ds.date,
    coalesce(fees.fees, 0) as fees,
    coalesce(tvl.tokenized_mcap_change, 0) as tokenized_mcap_change,
    coalesce(tvl.tokenized_mcap, 0) as tokenized_mcap
from date_spine ds
left join fees on ds.date = fees.date
left join tvl on ds.date = tvl.date
where ds.date < to_date(sysdate())