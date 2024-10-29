-- depends_on {{ ref("ez_near_transactions") }}
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
    fundamental_data as ({{ get_fundamental_data_for_chain("near") }}),
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
    )


select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    wau,
    mau,
    fees_native,
    case when fees is null then fees_native * price else fees end as fees,
    avg_txn_fee,
    median_txn_fee,
    revenue_native,
    revenue,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    price,
    market_cap,
    fdmc,
    tvl,
    dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem,
    weekly_contracts_deployed,
    weekly_contract_deployers,
    p2p_native_transfer_volume,
    p2p_token_transfer_volume,
    p2p_stablecoin_transfer_volume,
    p2p_transfer_volume,
    blob_fees_native,
    blob_fees,
    blob_size_mib,
    avg_mib_per_second,
    avg_cost_per_mib_native,
    avg_cost_per_mib,
    submitters
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join revenue_data on fundamental_data.date = revenue_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join da_metrics on fundamental_data.date = da_metrics.date
where fundamental_data.date < to_date(sysdate())
