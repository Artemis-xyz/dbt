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
    distinct_dates as (
    select distinct 
        block_timestamp AS date,
        sender
        from {{ ref("ez_sui_transactions") }}
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
    rolling_mau as (
        select 
        t1.date,
        count(distinct t2.sender) as mau
        from distinct_dates t1
        join distinct_dates t2 on t2.date between dateadd(DAY, -29, t1.date) and t1.date
        group by t1.date
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
left join rolling_mau on fundamental_data.date = rolling_mau.date
where fundamental_data.date < to_date(sysdate())
