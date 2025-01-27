{{
    config(
        materialized="table",
        database = 'RESERVE',
        schema = 'core',
        snowflake_warehouse = 'RESERVE',
        alias = 'ez_metrics'
    )
}}

with date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between '2021-10-01' and to_date(sysdate())
)
, dau as (
    select
        date,
        dau
    from {{ ref("fact_reserve_dau") }}
)
, tvl as (
    select
        date,
        tvl
    from {{ ref("fact_reserve_tvl") }}
)

select
    date,
    tvl,
    dau
from date_spine
left join tvl using (date)
left join dau using (date)