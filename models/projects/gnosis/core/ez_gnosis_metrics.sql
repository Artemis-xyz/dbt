{{
    config(
        materialized="incremental"
        , snowflake_warehouse="GNOSIS"
        , database="gnosis"
        , schema="core"
        , alias="ez_metrics"
        , incremental_strategy="merge"
        , unique_key="date"
        , on_schema_change="append_new_columns"
        , merge_update_columns=var("backfill_columns", [])
        , merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none
        , full_refresh=var("full_refresh", false)
        , tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

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
    , market_metrics as ({{ get_coingecko_metrics("gnosis") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("gnosis") }})
    , gnosis_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_gnosis_daily_dex_volumes") }}
    )

select
    f.date
    , 'gnosis' as artemis_id

    -- Standardized Metrics
    -- Market Data Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Chain Usage Metrics
    , f.txns AS chain_txns
    , f.dau AS chain_dau
    , f.dau
    , r.mau AS chain_mau
    , r.wau AS chain_wau
    , f.fees / f.txns AS chain_avg_txn_fee
    , gnosis_dex_volumes.dex_volumes AS chain_spot_volume

    -- Cashflow metrics
    , f.fees as chain_fees
    , f.fees AS fees
    , f.fees_native AS fees_native
    , f.revenue AS burned_fee_allocation

    -- Financial Metrics
    , f.revenue

    -- Developer Metrics
    , g.weekly_commits_core_ecosystem
    , g.weekly_commits_sub_ecosystem
    , g.weekly_developers_core_ecosystem
    , g.weekly_developers_sub_ecosystem

    -- Timestamp Columns
    , sysdate() as created_on
    , sysdate() as modified_on
from fundamental_data f
left join github_data g using (date)
left join defillama_data using (date)
left join market_metrics using (date)
left join rolling_metrics r using (date)
left join gnosis_dex_volumes using (date)
where true
{{ ez_metrics_incremental('f.date', backfill_date) }}
and f.date < to_date(sysdate())