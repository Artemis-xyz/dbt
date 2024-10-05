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
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , dex_volumes
    , price
    , market_cap
    , fdmc
from fundamental_data
left join github_data using (date)
left join defillama_data using (date)
left join price_data using (date)
left join rolling_metrics using (date)
where fundamental_data.date < to_date(sysdate())