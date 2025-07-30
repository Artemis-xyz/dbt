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
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as (
        select
            date
            , coalesce(txns, 0) as txns
            , coalesce(dau, 0) as dau
            , coalesce(gas_usd, 0) as fees
            , coalesce(gas, 0) as fees_native
        from {{ ref("fact_fuse_daa_txns_gas_gas_usd") }}
    )
    , github_data as ({{ get_github_metrics("fuse") }})
    , defillama_data as ({{ get_defillama_metrics("fuse") }})
    , market_metrics as ({{ get_coingecko_metrics("fuse-network-token") }})
select
    fundamental_data.date
    , 'fuse' as artemis_id
    
    -- Standardized Metrics
    
    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume
    
    -- Usage Metrics
    , fundamental_data.dau as chain_dau
    , fundamental_data.dau as dau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns
    , fundamental_data.avg_txn_fee as chain_avg_txn_fee
    , fundamental_data.dex_volumes as chain_spot_volume
    , defillama_data.tvl as chain_tvl
    , defillama_data.tvl as tvl

    -- Cash Flow Metrics
    , fundamental_data.fees_native as fees_native
    , fundamental_data.fees as chain_fees
    , fundamental_data.fees as fees
    
    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join github_data using (date)
left join defillama_data using (date)
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())