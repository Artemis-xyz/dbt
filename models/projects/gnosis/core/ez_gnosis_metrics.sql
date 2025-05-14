{{
    config(
        materialized="table"
        , snowflake_warehouse="GNOSIS"
        , database="gnosis"
        , schema="core"
        , alias="ez_metrics"
    )
}}


with
    fundamental_data as (
        select
            date
            , txns
            , daa as dau
            , gas_usd as fees
            , gas as fees_native
            , native_token_burn as revenue
            , revenue
        from {{ ref("fact_gnosis_daa_txns_gas_gas_usd") }}
        left join {{ ref("agg_daily_gnosis_revenue") }} using (date)
    )
    , github_data as ({{ get_github_metrics("gnosis") }})
    , defillama_data as ({{ get_defillama_metrics("gnosis") }})
    , price_data as ({{ get_coingecko_metrics("gnosis") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("gnosis") }})
    , gnosis_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_gnosis_daily_dex_volumes") }}
    )

select
    fundamental_data.date
    , 'gnosis' as chain
    , txns
    , dau
    , mau
    , wau
    , fees
    , fees / txns as avg_txn_fee
    , fees_native
    , revenue
    , revenue as revenue_native
    , dune_dex_volumes_gnosis.dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    , mau AS chain_mau
    , wau AS chain_wau
    , fees / txns AS chain_avg_txn_fee
    , dune_dex_volumes_gnosis.dex_volumes AS chain_spot_volume
    -- Cashflow metrics
    , fees as chain_fees
    , fees AS ecosystem_revenue
    , fees_native AS ecosystem_revenue_native
    , revenue AS burned_cash_flow
    , revenue_native AS burned_cash_flow_native
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
from fundamental_data
left join github_data using (date)
left join defillama_data using (date)
left join price_data using (date)
left join rolling_metrics using (date)
left join gnosis_dex_volumes as dune_dex_volumes_gnosis on fundamental_data.date = dune_dex_volumes_gnosis.date
where fundamental_data.date < to_date(sysdate())