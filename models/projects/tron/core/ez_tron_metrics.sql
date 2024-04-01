{{
    config(
        materialized="table",
        snowflake_warehouse="TRON",
        database="tron",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("tron") }}),
    price_data as ({{ get_coingecko_metrics("tron") }}),
    defillama_data as ({{ get_defillama_metrics("tron") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("tron") }}),
    github_data as ({{ get_github_metrics("tron") }})
select
    coalesce(
        fundamental_data.date,
        price_data.date,
        defillama_data.date,
        stablecoin_data.date,
        github_data.date
    ) as date,
    'tron' as chain,
    txns,
    dau,
    fees_native,
    fees_native as revenue_native,
    fees,
    fees as revenue,
    avg_txn_fee,
    returning_users,
    new_users,
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
    stablecoin_transfer_volume
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join github_data on fundamental_data.date = github_data.date
where fundamental_data.date < to_date(sysdate())
