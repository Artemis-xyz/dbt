-- depends_on {{ ref("ez_starknet_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="STARKNET",
        database="starknet",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("starknet") }}),
    price_data as ({{ get_coingecko_metrics("starknet") }}),
    defillama_data as ({{ get_defillama_metrics("starknet") }}),
    expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost
        from {{ ref("fact_starknet_l1_data_cost") }}
    ),
    github_data as ({{ get_github_metrics("starknet") }})

select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    fees,
    l1_data_cost_native,  -- fees paid to l1 by sequencer (L1 Fees)
    l1_data_cost,
    coalesce(fees_native, 0) -  l1_data_cost_native as revenue_native,  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    coalesce(fees, 0) -  l1_data_cost as revenue,
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
    weekly_developers_sub_ecosystem
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join github_data on fundamental_data.date = github_data.date
where fundamental_data.date < to_date(sysdate())
