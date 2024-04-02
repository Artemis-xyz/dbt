{{
    config(
        materialized="table",
        snowflake_warehouse="CELO",
        database="celo",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date, chain, gas_usd as fees, revenue, txns, dau, avg_txn_fee
        from {{ ref("fact_celo_dau_gas_usd_revenue_txns_avg_txns_fee") }}
    ),
    price_data as ({{ get_coingecko_metrics("celo") }}),
    defillama_data as ({{ get_defillama_metrics("celo") }}),
    github_data as ({{ get_github_metrics("celo") }})
select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    fees,
    revenue,
    avg_txn_fee,
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