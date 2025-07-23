{{
    config(
        materialized="incremental",
        snowflake_warehouse="FUSE",
        database="fuse",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
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
        from {{ ref("fact_fuse_daa_txns_gas_gas_usd") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , github_data as ({{ get_github_metrics("fuse") }})
    , defillama_data as ({{ get_defillama_metrics("fuse") }})
    , price_data as ({{ get_coingecko_metrics("fuse-network-token") }})
select
    fundamental_data.date
    , 'fuse' as chain
    , txns
    , dau
    , fees
    , case when txns > 0 then fees / txns end as avg_txn_fee
    , fees_native
    , dex_volumes
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    , avg_txn_fee as chain_avg_txn_fee
    , dex_volumes as chain_spot_volume
    -- Cash Flow Metrics
    , fees as chain_fees
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    -- Crypto Metrics
    , tvl
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , token_turnover_circulating
    , token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join github_data using (date)
left join defillama_data using (date)
left join price_data using (date)
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
    and fundamental_data.date < to_date(sysdate())