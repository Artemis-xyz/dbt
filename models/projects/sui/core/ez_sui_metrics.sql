{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
        database="sui",
        schema="core",
        alias="ez_metrics",
    )
}}

-- Based on the `get_fundamental_data_for_chain` macro in the primary
-- repo

with
    min_date as (
        select min(raw_date) as start_date, from_address
        from {{ source("PROD_LANDING", "fact_sui_transactions_silver_bigquery_v2") }}
        group by from_address
    ),
    new_users as (
        select count(distinct from_address) as new_users, start_date
        from min_date
        group by start_date
    ),
    chain_agg as (
        select
            raw_date as date,
            max(chain) as chain,
            sum(tx_fee) fees_native,
            sum(gas_usd) fees,
            sum(native_revenue) as revenue_native,
            sum(revenue) as revenue,
            count(*) txns,
            sum(gas_usd) / count(*) as avg_txn_fee,
            count(distinct from_address) dau
        from {{ source("PROD_LANDING", "fact_sui_transactions_silver_bigquery_v2") }}
        group by raw_date
    ),
    rolling_active_addresses as (
        select date, mau, wau
        from {{ ref("fact_sui_rolling_active_addresses") }}
    )
    , epoch_staking_data as (
        select
            date,
            stake_subsidy_amount as mints_native
        from {{ source("PROD_LANDING", "fact_sui_epoch_data") }}
    ), fundamental_data as (
        select
            TO_TIMESTAMP_NTZ(chain_agg.date) as date,
            chain,
            txns,
            dau,
            wau,
            mau,
            fees_native,
            fees,
            revenue_native,
            revenue,
            avg_txn_fee,
            (dau - new_users.new_users) as returning_users,
            new_users.new_users,
            epoch_staking_data.mints_native
        from chain_agg
        left join new_users on chain_agg.date = new_users.start_date
        left join rolling_active_addresses on chain_agg.date = rolling_active_addresses.date
        left join epoch_staking_data on chain_agg.date = epoch_staking_data.date
        where chain_agg.date < current_date()
    ),
    price_data as ({{ get_coingecko_metrics("sui") }}),
    defillama_data as ({{ get_defillama_metrics("sui") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("sui") }}),
    github_data as ({{ get_github_metrics("sui") }})
    , supply_data as (
        select 
            date
            , premine_unlocks_native
            , net_supply_change_native
            , circulating_supply_native
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
    , fees_native AS gross_protocol_revenue_native
    , fees AS gross_protocol_revenue
    , revenue AS burned_cash_flow
    , revenue_native AS burned_cash_flow_native
    , avg_txn_fee AS chain_avg_txn_fee
    -- Supply Metrics
    , mints_native as gross_emissions_native
    , premine_unlocks_native
    , net_supply_change_native
    , circulating_supply_native
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