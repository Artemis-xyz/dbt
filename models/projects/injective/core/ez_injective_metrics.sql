{{
    config(
        materialized="table",
        snowflake_warehouse="INJECTIVE",
        database="injective",
        schema="core",
        alias="ez_metrics",
    )
}}

with fundamental_data as (
    select
        TO_TIMESTAMP_NTZ(date) AS date, 
        * EXCLUDE date
    from {{ source('PROD_LANDING', 'ez_injective_metrics') }}
)
, daily_txns as (
    select * 
    from {{ ref("fact_injective_daily_txns_silver") }}
)
, revenue as (
    select
        date,
        revenue,
        revenue_native,
        revenue * 5/3 as spot_fees,
        revenue as auction_fees,
        (revenue * 5/3) * 2/5 as dapp_fees
    from {{ ref("fact_injective_revenue_silver") }}
)
, mints AS (
    SELECT *
    FROM {{ ref("fact_injective_mints_silver") }}
)
, unlocks as (
    select
        date,
        outflows
    from {{ ref("fact_injective_unlocks") }}
)
, defillama_metrics as (
    select
        date,
        tvl as defillama_tvl,
        dex_volumes as defillama_dex_volumes
    from {{ ref("fact_injective_defillama_tvl_and_dexvolumes") }}
)
, date_spine as (
    select * from {{ ref('dim_date_spine') }}
    where date between (select min(date) from unlocks) and to_date(sysdate())
)
, market_metrics as ({{ get_coingecko_metrics("injective-protocol") }})


select

    date_spine.date
    -- Old metrics needed for compatibility
    , fundamental_data.dau
    , fundamental_data.wau
    , fundamental_data.mau
    , fundamental_data.txns
    , fundamental_data.fees
    , fundamental_data.fees_native
    , fundamental_data.avg_txn_fee
    , unlocks.outflows as unlocks
    , mints.mints as gross_emissions_native
    , COALESCE(revenue.revenue, 0) AS revenue
    , fundamental_data.fees_native as ecosystem_revenue_native
    , coalesce(revenue.revenue_native, 0) as burned_cash_flow_native
    
    -- Standardized Metrics

    -- Market Data Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc

    -- Chain Usage Metrics
    , fundamental_data.txns as chain_txns
    , fundamental_data.dau as chain_dau
    , fundamental_data.mau as chain_mau
    , fundamental_data.wau as chain_wau
    , fundamental_data.returning_users
    , fundamental_data.new_users
    , fundamental_data.fees / fundamental_data.txns as chain_avg_txn_fee
    , defillama_metrics.defillama_tvl as tvl
    , defillama_metrics.defillama_dex_volumes as spot_volume
    , null as low_sleep_users
    , null as high_sleep_users
    , null as sybil_users
    , null as non_sybil_users

    -- Cashflow Metrics
    , coalesce(fundamental_data.fees, 0) as chain_fees
    , coalesce(revenue.spot_fees, 0) as spot_fees
    , (coalesce(revenue.spot_fees, 0) + coalesce(fundamental_data.fees, 0)) as ecosystem_revenue
    , coalesce(revenue.auction_fees, 0) as burned_cash_flow
    , coalesce(fundamental_data.fees, 0) as validator_cash_flow
    , coalesce(revenue.dapp_fees, 0) as dapp_cash_flow

    -- INJ Token Supply Data
    , coalesce(mints.mints, 0) as emissions_native
    , coalesce(unlocks.outflows, 0) as premine_unlocks_native
    , coalesce(revenue.revenue_native, 0) as burns_native
    , coalesce(mints.mints, 0) + coalesce(unlocks.outflows, 0) - coalesce(revenue.revenue_native, 0) as net_supply_change_native
    , sum(coalesce(mints.mints, 0) + coalesce(unlocks.outflows, 0) - coalesce(revenue.revenue_native, 0)) over (order by date_spine.date) as circulating_supply_native

from date_spine
left join market_metrics using (date)
left join fundamental_data using (date)
left join defillama_metrics using (date)
left join revenue using (date)
left join mints using (date)
left join unlocks using (date)
