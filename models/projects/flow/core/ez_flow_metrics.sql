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

with fees_revenue_data as (
    select 
        date, 
        total_fees_usd as fees, 
        fees_burned_usd as revenue
    from {{ ref("fact_flow_fees_revs") }}
)
,    dau_txn_data as (
    select 
        date, 
        chain, 
        daa as dau, 
        txns 
    from {{ ref("fact_flow_daa_txns") }}
)
, daily_supply_data as (
    select
        date,
        emissions_native,
        premine_unlocks_native,
        burns_native,
        net_supply_change_native,
        circulating_supply
    from {{ ref('fact_flow_daily_supply_data') }}
)
, date_spine as (
    select
        ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from dau_txn_data) and to_date(sysdate())
)
,    market_metrics as ({{ get_coingecko_metrics("flow") }})
,    defillama_data as ({{ get_defillama_metrics("flow") }})
,    github_data as ({{ get_github_metrics("flow") }})
,    nft_metrics as ({{ get_nft_metrics("flow") }})

select
    date_spine.date

    --Old metrics needed for compatibility
    , dau_txn_data.txns
    , dau_txn_data.dau
    , fees_revenue_data.fees
    , fees_revenue_data.fees / dau_txn_data.txns as avg_txn_fee
    , fees_revenue_data.revenue
    , dau_txn_data.chain
    , defillama_data.dex_volumes as dex_volumes
    , defillama_data.tvl as tvl
    , nft_metrics.nft_trading_volume
    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Metrics
    , dau_txn_data.txns as chain_txns
    , dau_txn_data.dau as chain_dau
    , fees_revenue_data.fees / dau_txn_data.txns as chain_avg_txn_fee
    , defillama_data.dex_volumes as chain_spot_volume
    , defillama_data.tvl as chain_tvl
    , nft_metrics.nft_trading_volume as chain_nft_trading_volume

    -- Cashflow metrics
    , fees_revenue_data.fees AS chain_fees
    , fees_revenue_data.fees AS ecosystem_revenue
    , fees_revenue_data.fees AS validator_cash_flow
    , fees_revenue_data.revenue AS burned_cash_flow

    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem

    --FLOW Token Supply Data
    , daily_supply_data.emissions_native
    , daily_supply_data.premine_unlocks_native
    , daily_supply_data.burns_native
    , daily_supply_data.net_supply_change_native
    , daily_supply_data.circulating_supply

from date_spine
left join dau_txn_data on date_spine.date = dau_txn_data.date
left join fees_revenue_data on date_spine.date = fees_revenue_data.date
left join market_metrics on date_spine.date = market_metrics.date
left join daily_supply_data on date_spine.date = daily_supply_data.date
left join defillama_data on date_spine.date = defillama_data.date
left join github_data on date_spine.date = github_data.date
left join nft_metrics on date_spine.date = nft_metrics.date
where date_spine.date < to_date(sysdate())
