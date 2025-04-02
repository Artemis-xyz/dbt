--depends_on: {{ ref("fact_flow_nft_trading_volume") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="FLOW",
        database="flow",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fees_revenue_data as (
        select date, total_fees_usd as fees, fees_burned_usd as revenue
        from {{ ref("fact_flow_fees_revs") }}
    ),
    dau_txn_data as (
        select date, chain, daa as dau, txns from {{ ref("fact_flow_daa_txns") }}
    ),
    price_data as ({{ get_coingecko_metrics("flow") }}),
    defillama_data as ({{ get_defillama_metrics("flow") }}),
    github_data as ({{ get_github_metrics("flow") }}),
    nft_metrics as ({{ get_nft_metrics("flow") }})
select
    dau_txn_data.date
    , txns
    , dau
    , fees
    , fees / txns as avg_txn_fee
    , revenue
    , dau_txn_data.chain
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    -- Cashflow metrics
    , fees AS gross_protocol_revenue
    , revenue AS burned_cash_flow
    -- Chain Metrics
    , fees / txns AS chain_avg_txn_fee
    , dex_volumes
    , nft_trading_volume
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
from dau_txn_data
left join fees_revenue_data on dau_txn_data.date = fees_revenue_data.date
left join price_data on dau_txn_data.date = price_data.date
left join defillama_data on dau_txn_data.date = defillama_data.date
left join github_data on dau_txn_data.date = github_data.date
left join nft_metrics on dau_txn_data.date = nft_metrics.date
where dau_txn_data.date < to_date(sysdate())
