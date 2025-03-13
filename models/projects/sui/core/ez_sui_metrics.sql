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
select
    fundamental_data.date,
    'sui' as chain,
    avg_txn_fee,
    txns,
    dau,
    wau,
    mau,
    new_users,
    returning_users,
    fees_native,
    fees,
    revenue_native,
    revenue,
    mints_native,
    price,
    market_cap,
    fdmc,
    tvl,
    dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem,
    stablecoin_total_supply,
    stablecoin_txns,
    stablecoin_dau,
    stablecoin_mau,
    stablecoin_transfer_volume,
    artemis_stablecoin_txns,
    artemis_stablecoin_dau,
    artemis_stablecoin_mau,
    artemis_stablecoin_transfer_volume,
    p2p_stablecoin_txns,
    p2p_stablecoin_dau,
    p2p_stablecoin_mau,
    stablecoin_data.p2p_stablecoin_transfer_volume,
    stablecoin_tokenholder_count,
    p2p_stablecoin_tokenholder_count
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join github_data on fundamental_data.date = github_data.date
where fundamental_data.date < to_date(sysdate())