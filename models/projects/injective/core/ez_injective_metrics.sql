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
    unlocks as (select * from {{ ref("fact_injective_unlocks") }}),
    price_data as ({{ get_coingecko_metrics("injective-protocol") }})
select
    fundamental_data.date
    , 'injective' as chain
    , fundamental_data.dau
    , fundamental_data.wau
    , fundamental_data.mau
    , fundamental_data.txns
    , fundamental_data.fees
    , fundamental_data.fees_native
    , fundamental_data.avg_txn_fee
    , unlocks.outflows AS unlocks
    , mints.mints AS mints
    , COALESCE(revenue.revenue, 0) AS revenue
    , COALESCE(revenue.revenue_native, 0) AS burns_native
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    , mau AS chain_mau
    , wau AS chain_wau
    , fundamental_data.returning_users
    , fundamental_data.new_users
    , fees / txns AS chain_avg_txn_fee
    , null AS low_sleep_users
    , null AS high_sleep_users
    , null AS sybil_users
    , null AS non_sybil_users
    -- Cashflow Metrics
    , fees AS gross_protocol_revenue
    , fees_native AS gross_protocol_revenue_native
    , coalesce(revenue.revenue, 0) AS burned_cash_flow
    , coalesce(revenue.revenue_native, 0) AS burned_cash_flow_native
    -- Supply Metrics
    , unlocks.outflows AS emissions_native
    , mints.mints AS mints_native
from fundamental_data
left join revenue using (date)
left join mints using (date)
left join unlocks using (date)
left join price_data using (date)