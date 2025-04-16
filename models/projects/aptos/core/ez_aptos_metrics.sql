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
            date
            , chain
            , txns
            , daa as dau
            , gas as fees_native
            , gas_usd as fees
            , fees / txns as avg_txn_fee
            , revenue
            , gas as revenue_native
        from {{ ref("fact_aptos_daa_txns_gas_gas_usd_revenue") }}
    )
    , price_data as ({{ get_coingecko_metrics("aptos") }})
    , defillama_data as ({{ get_defillama_metrics("aptos") }})
    , github_data as ({{ get_github_metrics("aptos") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("aptos") }})
    , aptos_dex_volumes as (
        select 
            date
            , volume_usd as dex_volumes
        from {{ ref("fact_aptos_dex_volumes") }}
    )
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
    , aptos_dex_volumes.dex_volumes

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(price_data.price, 0) as price
    , coalesce(price_data.market_cap, 0) as market_cap
    , coalesce(price_data.fdmc, 0) as fdmc
    , coalesce(price_data.token_volume, 0) as token_volume

    -- Chain Metrics
    , coalesce(dau, 0) as chain_dau
    , coalesce(wau, 0) as chain_wau
    , coalesce(mau, 0) as chain_mau
    , coalesce(txns, 0) as chain_txns
    , coalesce(avg_txn_fee, 0) as chain_avg_txn_fee
    , coalesce(aptos_dex_volumes.dex_volumes, 0) as chain_dex_volume

    -- Crypto Metrics
    , coalesce(tvl, 0) as tvl

    -- Cash Flow Metrics
    , coalesce(fees, 0) as chain_fees
    , coalesce(fees, 0) as gross_protocol_revenue
    , coalesce(fees_native, 0) as gross_protocol_revenue_native
    , coalesce(fees, 0) as burned_cash_flow
    , coalesce(fees_native, 0) as burned_cash_flow_native

    -- Developer Metrics
    , coalesce(weekly_commits_core_ecosystem, 0) as weekly_commits_core_ecosystem
    , coalesce(weekly_commits_sub_ecosystem, 0) as weekly_commits_sub_ecosystem
    , coalesce(weekly_developers_core_ecosystem, 0) as weekly_developers_core_ecosystem
    , coalesce(weekly_developers_sub_ecosystem, 0) as weekly_developers_sub_ecosystem

    -- Turnover Metrics
    , coalesce(price_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(price_data.token_turnover_fdv, 0) as token_turnover_fdv
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join aptos_dex_volumes on fundamental_data.date = aptos_dex_volumes.date
where fundamental_data.date < to_date(sysdate())
