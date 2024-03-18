-- depends_on {{ ref("ez_arbitrum_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("arbitrum") }}),
    price_data as ({{ get_coingecko_metrics("arbitrum") }}),
    defillama_data as ({{ get_defillama_metrics("arbitrum") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("arbitrum") }}),
    expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost, revenue_native, revenue
        from {{ ref("agg_daily_arbitrum_revenue") }}
    ),
    github_data as ({{ get_github_metrics("arbitrum") }}),
    contract_data as ({{ get_contract_metrics("arbitrum") }})
select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    fees_native,  -- total gas fees paid on l2 by users(L2 Fees)
    fees,
    l1_data_cost_native,  -- fees paid to l1 by sequencer (L1 Fees)
    l1_data_cost,
    revenue_native,  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    revenue,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    dau_over_100,
    price,
    market_cap,
    fdmc,
    tvl,
    dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem,
    weekly_contracts_deployed,
    weekly_contract_deployers,
    stablecoin_total_supply,
    stablecoin_txns,
    stablecoin_dau,
    stablecoin_transfer_volume
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
where fundamental_data.date < to_date(sysdate())
