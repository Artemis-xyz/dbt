-- depends_on {{ ref("fact_near_transactions_gold") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="NEAR",
        database="near",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("near") }}),
    price_data as ({{ get_coingecko_metrics("near") }}),
    defillama_data as ({{ get_defillama_metrics("near") }}),
    revenue_data as (
        select date, revenue_native, revenue from {{ ref("fact_near_revenue_gold") }}
    ),
    github_data as ({{ get_github_metrics("near") }}),
    contract_data as ({{ get_contract_metrics("near") }}),
    p2p_metrics as ({{ get_p2p_metrics("near") }})

select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    fees_native,
    case when fees is null then fees_native * price else fees end as fees,
    avg_txn_fee,
    revenue_native,
    revenue,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
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
    p2p_native_transfer_volume,
    p2p_token_transfer_volume,
    p2p_stablecoin_transfer_volume,
    p2p_transfer_volume
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join revenue_data on fundamental_data.date = revenue_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
where fundamental_data.date < to_date(sysdate())
