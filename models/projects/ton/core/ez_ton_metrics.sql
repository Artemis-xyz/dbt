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
            chain,
            txns,
            dau,
            fees,
            fees_native,
            revenue,
            revenue_native,
            avg_txn_fee
        from {{ ref("fact_ton_daa_txns_gas_gas_usd_revenue_revenue_native") }}
    ),
    price_data as ({{ get_coingecko_metrics("the-open-network") }}),
    defillama_data as ({{ get_defillama_metrics("ton") }}),
    dex_data as (
        select
            date,
            dex_volumes
        from {{ ref("fact_ton_dex_volumes") }}
    ),
    github_data as ({{ get_github_metrics("ton") }})
select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    fees_native,
    fees,
    revenue_native,
    revenue,
    avg_txn_fee,
    price,
    market_cap,
    fdmc,
    tvl,
    dex_data.dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join dex_data on fundamental_data.date = dex_data.date
where fundamental_data.date < to_date(sysdate())
