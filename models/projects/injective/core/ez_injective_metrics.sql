{{
    config(
        materialized="table",
        snowflake_warehouse="INJECTIVE",
        database="injective",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    dau as (select * from {{ ref("fact_injective_dau_silver") }}),
    daily_txns as (select * from {{ ref("fact_injective_daily_txns_silver") }}),
    fees_native as (select * from {{ ref("fact_injective_fees_native_silver") }}),
    fees_usd as (select * from {{ ref("fact_injective_fees_usd_silver") }}),
    revenue as (select * from {{ ref("fact_injective_revenue_silver") }}),
    mints as (select * from {{ ref("fact_injective_mints_silver") }})

select
    dau.date,
    'injective' as chain,
    dau.dau,
    daily_txns.txns,
    fees_usd.fees,
    fees_native.fees_native,
    coalesce(revenue.revenue, 0) as revenue,
    mints.mints
from dau
left join daily_txns on dau.date = daily_txns.date
left join fees_usd on dau.date = fees_usd.date
left join fees_native on dau.date = fees_native.date
left join revenue on dau.date = revenue.date
left join mints on dau.date = mints.date
