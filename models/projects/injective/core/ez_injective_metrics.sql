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
    -- Alternative fundamental source from BigQuery, preferred when possible over Snowflake data
    fundamental_data as (select * EXCLUDE date, TO_TIMESTAMP_NTZ(date) AS date from {{ source('PROD_LANDING', 'ez_injective_metrics') }}),
    daily_txns as (select * from {{ ref("fact_injective_daily_txns_silver") }}),
    revenue as (select * from {{ ref("fact_injective_revenue_silver") }}),
    mints as (select * from {{ ref("fact_injective_mints_silver") }}),
    unlocks as (select * from {{ ref("fact_injective_unlocks") }})
select
    fundamental_data.date,
    'injective' as chain,
    fundamental_data.dau,
    fundamental_data.wau,
    fundamental_data.mau,
    fundamental_data.txns,
    fundamental_data.fees,
    fundamental_data.fees_native,
    fundamental_data.avg_txn_fee,
    fundamental_data.returning_users,
    fundamental_data.new_users,
    unlocks.outflows as unlocks,
    coalesce(revenue.revenue, 0) as revenue,
    coalesce(revenue.revenue_native, 0) as burns_native,
    mints.mints,
    null as low_sleep_users,
    null as high_sleep_users,
    null as sybil_users,
    null as non_sybil_users
from fundamental_data
left join revenue on fundamental_data.date = revenue.date
left join mints on fundamental_data.date = mints.date
left join unlocks on fundamental_data.date = unlocks.date