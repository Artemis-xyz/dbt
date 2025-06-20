{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
        database="sui",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (select * EXCLUDE date, TO_TIMESTAMP_NTZ(date) AS date from {{ source('PROD_LANDING', 'ez_sui_metrics') }}),
    price_data as ({{ get_coingecko_metrics("sui") }}),
    defillama_data as ({{ get_defillama_metrics("sui") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("sui") }}),
    github_data as ({{ get_github_metrics("sui") }})
    , supply_data as (
        select 
            date
            , max_supply_native
            , total_supply_native
            , foundation_owned_supply_native
            , unvested_tokens_native
        from {{ ref("fact_sui_supply_data") }}
    )
select
    fundamental_data.date
    , 'sui' as chain
    , avg_txn_fee
    , txns
    , dau
    , wau
    , mau
    , fees_native
    , fees
    , revenue_native
    , revenue
    , dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , txns AS chain_txns
    , dex_volumes AS chain_spot_volume
    , returning_users
    , new_users
    -- Cashflow Metrics
    , fees as chain_fees
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
    , revenue AS burned_fee_allocation
    , revenue_native AS burned_fee_allocation_native
    , avg_txn_fee AS chain_avg_txn_fee
    -- Supply Metrics
    , max_supply_native
    , total_supply_native
    , total_supply_native - foundation_owned_supply_native - burned_fee_allocation_native as issued_supply_native
    , total_supply_native - foundation_owned_supply_native - burned_fee_allocation_native - unvested_tokens_native as circulating_supply_native
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    -- Stablecoin Metrics
    , stablecoin_total_supply
    , stablecoin_txns
    , stablecoin_dau
    , stablecoin_mau
    , stablecoin_transfer_volume
    , stablecoin_tokenholder_count
    , artemis_stablecoin_txns
    , artemis_stablecoin_dau
    , artemis_stablecoin_mau
    , artemis_stablecoin_transfer_volume
    , p2p_stablecoin_tokenholder_count
    , p2p_stablecoin_txns
    , p2p_stablecoin_dau
    , p2p_stablecoin_mau
    , p2p_stablecoin_transfer_volume
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join github_data on fundamental_data.date = github_data.date
left join supply_data on fundamental_data.date = supply_data.date
where fundamental_data.date < to_date(sysdate())