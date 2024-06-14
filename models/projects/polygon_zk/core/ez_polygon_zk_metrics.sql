{{
    config(
        materialized="table",
        snowflake_warehouse="POLYGON_ZK",
        database="polygon_zk",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select date, chain, daa as dau, txns, gas_usd as fees
        from {{ ref("fact_polygon_zk_daa_txns_gas_usd") }}
    ),
    price_data as ({{ get_coingecko_metrics("matic-network") }}),
    defillama_data as ({{ get_defillama_metrics("polygon zkevm") }}),
    revenue_data as (
        select
            date,
            expenses_native as l1_data_cost_native,
            expenses as l1_data_cost,
            revenue
        from {{ ref("agg_daily_polygon_zk_revenue") }}
    ),
    github_data as ({{ get_github_metrics("Polygon Hermez") }})
select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    l1_data_cost_native,
    l1_data_cost,
    fees,
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
left join revenue_data on fundamental_data.date = revenue_data.date
left join github_data on fundamental_data.date = github_data.date
where fundamental_data.date < to_date(sysdate())
