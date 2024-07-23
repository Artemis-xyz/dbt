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
    ), ton_app_daa as (
        select 
            date,
            daa as dau
        from {{ ref("fact_ton_app_daa") }}
    ),
    ton_app_txns_fees as (
        select 
            date,
            txns,
            fees_native,
            avg_txn_fee_native,
        from {{ ref("fact_ton_app_fees_txns") }}
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
    ton_app_daa.date,
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
from ton_app_daa
left join price_data on ton_app_daa.date = price_data.date
left join defillama_data on ton_app_daa.date = defillama_data.date
left join github_data on ton_app_daa.date = github_data.date
left join dex_data on ton_app_daa.date = dex_data.date
left join fundamental_data on ton_app_daa.date = fundamental_data.date
left join ton_app_txns_fees on ton_app_daa.date = ton_app_txns_fees.date
where ton_app_daa.date < to_date(sysdate())
