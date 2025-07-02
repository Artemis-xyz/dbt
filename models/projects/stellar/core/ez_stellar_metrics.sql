{{
    config(
        materialized="table",
        snowflake_warehouse="STELLAR",
        database="stellar",
        schema="core",
        alias="ez_metrics",
    )
}}
with fundamental_data as (
    select 
        * EXCLUDE date,
        TO_TIMESTAMP_NTZ(date) AS date 
    from {{ source('PROD_LANDING', 'ez_stellar_metrics') }}
)
, rwa_tvl as (
    select * from {{ ref('fact_stellar_rwa_tvl') }}
)
, stablecoin_tvl as (
    -- Sum mktcap in USD across all stablecoins 
    select 
        date,
        sum(total_circulating_usd) as stablecoin_mc
    from {{ ref ('fact_stellar_stablecoin_tvl') }} 
    group by 
        date 
)
, issued_supply_metrics as (
    select 
        date,
        max_supply as max_supply_native,
        total_supply as total_supply_native,
        issued_supply as issued_supply_native,
        circulating_supply_native as circulating_supply_native
    from {{ ref('fact_stellar_issued_supply_and_float') }}
)
, prices as ({{ get_coingecko_price_with_latest("stellar") }})
, price_data as ( {{ get_coingecko_metrics("stellar") }} )
select
    fundamental_data.date
    , fundamental_data.chain
    , fundamental_data.classic_txns AS txns
    , fundamental_data.soroban_txns AS soroban_txns
    , fundamental_data.daily_fees as fees_native
    , fundamental_data.daily_fees * prices.price as fees
    , fundamental_data.assets_deployed as assets_deployed
    , fundamental_data.operations as operations
    , fundamental_data.active_contracts as active_contracts
    , fundamental_data.dau as dau
    , fundamental_data.wau as wau
    , fundamental_data.mau as mau
    , fundamental_data.ledgers_closed as ledgers_closed
    , stablecoin_tvl.stablecoin_mc
    , rwa_tvl.rwa_tvl

    -- Standardized Metrics

    -- Market Data
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume

    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    , wau as chain_wau
    , mau as chain_mau
    , fundamental_data.returning_users as returning_users
    , fundamental_data.new_users as new_users
    , null as low_sleep_users
    , null as high_sleep_users
    , null as sybil_users
    , null as non_sybil_users

    -- Cash Flow Metrics
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native

    -- Issued Supply Metrics
    , issued_supply_metrics.max_supply_native
    , issued_supply_metrics.total_supply_native
    , issued_supply_metrics.issued_supply_native
    , issued_supply_metrics.circulating_supply_native

    -- Financial Statement Metrics
    , fees as revenue

    -- Real World Asset Metrics
    , rwa_tvl as tokenized_market_cap

    -- Stablecoin Metrics
    , stablecoin_mc as stablecoin_total_supply
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv

from fundamental_data
left join prices using(date)
left join price_data on fundamental_data.date = price_data.date
left join rwa_tvl on fundamental_data.date = rwa_tvl.date
left join stablecoin_tvl on fundamental_data.date = stablecoin_tvl.date
left join issued_supply_metrics on fundamental_data.date = issued_supply_metrics.date