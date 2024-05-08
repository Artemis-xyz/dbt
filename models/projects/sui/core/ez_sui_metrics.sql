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
    min_date as (
        select min(block_timestamp) as start_timestamp, sender
        from {{ ref("ez_sui_transactions") }}
        group by sender
    ),
    new_users as (
        select
            count(distinct sender) as new_users,
            date_trunc('day', start_timestamp) as start_date
        from min_date
        group by start_date
    ),
    fundamental_data as (
        select
            raw_date as date,
            chain,
            count(*) as txns,
            count(distinct sender) as dau,
            sum(tx_fee) as fees_native,
            sum(gas_usd) as fees,
            sum(revenue) as revenue,
            sum(native_revenue) as revenue_native
        from {{ ref("ez_sui_transactions") }}
        group by raw_date, chain
    ),
    price_data as ({{ get_coingecko_metrics("sui") }}),
    defillama_data as ({{ get_defillama_metrics("sui") }}),
    github_data as ({{ get_github_metrics("sui") }}),
    mau_metrics as (
        select
        date_trunc('month', raw_date) as month,
        count(distinct sender) as mau
        from {{ ref("ez_sui_transactions") }}
        group by month
    )
select
    fundamental_data.date,
    fundamental_data.chain,
    fees / txns as avg_txn_fee,
    txns,
    dau,
    mau,
    new_users,
    dau - new_users as returning_users,
    fees_native,
    fees,
    revenue_native,
    revenue,
    price,
    market_cap,
    fdmc,
    tvl,
    dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join new_users on fundamental_data.date = new_users.start_date
left join mau_metrics on fundamental_data.date = mau_metrics.month
where fundamental_data.date < to_date(sysdate())
