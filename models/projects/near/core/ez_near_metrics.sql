-- depends_on {{ ref("fact_near_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="NEAR",
        database="near",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("near", "v2") }}),
    price_data as ({{ get_coingecko_metrics("near") }}),
    defillama_data as ({{ get_defillama_metrics("near") }}),
    revenue_data as (
        select date, revenue_native, revenue from {{ ref("fact_near_revenue") }}
    ),
    github_data as ({{ get_github_metrics("near") }}),
    contract_data as ({{ get_contract_metrics("near") }}),
    p2p_metrics as ({{ get_p2p_metrics("near") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("near") }}),
    da_metrics as (
        select date, blob_fees_native, blob_fees, blob_size_mib, avg_mib_per_second, avg_cost_per_mib_native, avg_cost_per_mib, submitters
        from {{ ref("fact_near_da_metrics") }}
    ), 
    near_dex_volumes as (
        select date, volume_usd as dex_volumes
        from {{ ref("fact_near_dex_volumes") }}
    )

select
    fundamental_data.date
    , fundamental_data.chain
    , txns
    , dau
    , wau
    , mau
    , fees_native
    , fees
    , avg_txn_fee
    , median_txn_fee
    , revenue_native
    , revenue
    , coalesce(near_dex_volumes.dex_volumes, 0) as dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , dau as chain_dau
    , wau as chain_wau
    , mau as chain_mau
    , txns as chain_txns
    , avg_txn_fee AS chain_avg_txn_fee
    , returning_users
    , new_users
    , low_sleep_users
    , high_sleep_users
    -- Cashflow Metrics
    , case when fees is null then fees_native * price else fees end as chain_fees
    , fees_native as ecosystem_revenue_native
    , case when fees is null then fees_native * price else fees end as ecosystem_revenue
    , median_txn_fee AS chain_median_txn_fee
    , revenue_native AS burned_cash_flow_native
    , revenue AS burned_cash_flow
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers
    , p2p_native_transfer_volume
    , p2p_token_transfer_volume
    , p2p_stablecoin_transfer_volume
    , p2p_transfer_volume
    , blob_fees_native
    , blob_fees
    , blob_size_mib
    , avg_mib_per_second
    , avg_cost_per_mib_native
    , avg_cost_per_mib
    , submitters
    , coalesce(near_dex_volumes.dex_volumes, 0) as chain_spot_volume
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join revenue_data on fundamental_data.date = revenue_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join da_metrics on fundamental_data.date = da_metrics.date
left join near_dex_volumes on fundamental_data.date = near_dex_volumes.date
where fundamental_data.date < to_date(sysdate())
