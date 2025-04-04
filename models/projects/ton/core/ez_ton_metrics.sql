{{
    config(
        materialized="table",
        snowflake_warehouse="TON",
        database="ton",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select 
            date,
            txns as transaction_nodes
        from {{ ref("fact_ton_daa_txns_gas_gas_usd_revenue_revenue_native") }}
    ), 
    ton_apps_fundamental_data as (
        select 
            date
            , dau
            , fees_native
            , txns
            , avg_txn_fee_native
        from {{ ref("fact_ton_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics("the-open-network") }}),
    defillama_data as ({{ get_defillama_metrics("ton") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("ton") }}),
    github_data as ({{ get_github_metrics("ton") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("ton") }})
select
    ton.date
    , 'ton' as chain
    , dau
    , wau
    , mau
    , txns
    , fees_native
    , fees_native * price AS fees
    , fees_native /2 AS revenue_native
    , (fees_native / 2) * price AS revenue
    , avg_txn_fee_native * price AS avg_txn_fee
    -- Bespoke Metrics
    , transaction_nodes
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
    , avg_txn_fee_native * price AS chain_avg_txn_fee
    -- Cash Flow Metrics
    , fees_native AS gross_protocol_revenue_native
    , fees * price AS gross_protocol_revenue
    , fees_native / 2 AS burned_cash_flow_native
    , (fees_native / 2) * price AS burned_cash_flow
    , fees_native / 2 AS validator_cash_flow_native
    , (fees_native / 2) * price AS validator_cash_flow
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
    , p2p_stablecoin_txns
    , p2p_stablecoin_dau
    , p2p_stablecoin_mau
    , p2p_stablecoin_transfer_volume
    , p2p_stablecoin_tokenholder_count
from ton_apps_fundamental_data as ton
left join price_data on ton.date = price_data.date
left join defillama_data on ton.date = defillama_data.date
left join github_data on ton.date = github_data.date
left join fundamental_data on ton.date = fundamental_data.date
left join stablecoin_data on ton.date = stablecoin_data.date
left join rolling_metrics on ton.date = rolling_metrics.date
where ton.date < to_date(sysdate())
