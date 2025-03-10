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
    l1_data_cost as (
        select
            date,
            l1_data_cost_native,
            l1_data_cost
        from {{ ref("fact_polygon_zk_l1_data_cost") }}
    ),
    github_data as ({{ get_github_metrics("Polygon Hermez") }}),
    polygon_zk_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_polygon_zk_daily_dex_volumes") }}
    )
select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    l1_data_cost_native,
    l1_data_cost,
    fees,
    fees / txns as avg_txn_fee,
    coalesce(fees, 0) - l1_data_cost as revenue,
    price,
    market_cap,
    fdmc,
    tvl,
    dune_dex_volumes_polygon_zk.dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join l1_data_cost on fundamental_data.date = l1_data_cost.date
left join github_data on fundamental_data.date = github_data.date
left join polygon_zk_dex_volumes as dune_dex_volumes_polygon_zk on fundamental_data.date = dune_dex_volumes_polygon_zk.date
where fundamental_data.date < to_date(sysdate())
