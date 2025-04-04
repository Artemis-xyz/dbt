{{
    config(
        materialized="table",
        snowflake_warehouse="APTOS",
        database="aptos",
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
            daa as dau,
            gas as fees_native,
            gas_usd as fees,
            fees / txns as avg_txn_fee,
            revenue,
            gas as revenue_native
        from {{ ref("fact_aptos_daa_txns_gas_gas_usd_revenue") }}
    ),
    price_data as ({{ get_coingecko_metrics("aptos") }}),
    defillama_data as ({{ get_defillama_metrics("aptos") }}),
    github_data as ({{ get_github_metrics("aptos") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("aptos") }})
select
    fundamental_data.date
    , fundamental_data.chain
    , txns
    , dau
    , wau
    , mau
    , fees_native
    , fees
    , avg_txn_fee
    , revenue_native
    , revenue
    , dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , dex_volumes AS chain_dex_volumes
    , avg_txn_fee AS chain_avg_txn_fee
    -- Cashflow Metrics
    , fees AS gross_protocol_revenue
    , fees_native AS gross_protocol_revenue_native
    , revenue AS token_cash_flow
    , revenue_native AS token_cash_flow_native
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
where fundamental_data.date < to_date(sysdate())
