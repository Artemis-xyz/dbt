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
            txns as transaction_nodes
        from {{ ref("fact_ton_daa_txns_gas_gas_usd_revenue_revenue_native") }}
    ), ton_apps_fundamental_data as (
        select 
            date,
            , dau
            , fees_native
            , txns
            , avg_txn_fee_native
        from {{ ref("fact_ton_fundamental_metrics") }}
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
    ton.date,
    'ton' as chain,
    transaction_nodes,
    price,
    market_cap,
    fdmc,
    tvl,
    dex_data.dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem,
    dau,
    txns,
    fees_native,
    fees_native * price as fees,
    fees_native / 2 as revenue_native,
    (fees_native / 2) * price as revenue,
    avg_txn_fee_native * price as avg_txn_fee
from ton_apps_fundamental_data as ton
left join price_data on ton.date = price_data.date
left join defillama_data on ton.date = defillama_data.date
left join github_data on ton.date = github_data.date
left join dex_data on ton.date = dex_data.date
left join fundamental_data on ton.date = fundamental_data.date
where ton.date < to_date(sysdate())
