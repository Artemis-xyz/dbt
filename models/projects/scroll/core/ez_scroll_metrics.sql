{{
    config(
        materialized="incremental",
        snowflake_warehouse="SCROLL",
        database="scroll",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
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
            , median_gas_usd as median_txn_fee
            , gas as fees_native
            , revenue
            , revenue_native
            , l1_data_cost
            , l1_data_cost_native
        from {{ ref("fact_scroll_txns") }}
        left join {{ ref("fact_scroll_daa") }} using (date)
        left join {{ ref("fact_scroll_gas_gas_usd_revenue") }} using (date)
    )
    , github_data as ({{ get_github_metrics("scroll") }})
    , contract_data as ({{ get_contract_metrics("scroll") }})
    , defillama_data as ({{ get_defillama_metrics("scroll") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("scroll") }})
    , scroll_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_scroll_daily_dex_volumes") }}
    )
    , market_metrics as ({{ get_coingecko_metrics("scroll") }})
select
    fundamental_data.date
    , 'scroll' as artemis_id
    , 'scroll' as chain

    -- Standardized Metrics
    
    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume
    
    -- Usage Data
    , fundamental_data.dau as chain_dau
    , fundamental_data.dau as dau
    , rolling_metrics.wau as chain_wau
    , rolling_metrics.mau as chain_mau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns
    , scroll_dex_volumes.dex_volumes as chain_spot_volume
    , scroll_dex_volumes.adjusted_dex_volumes as adjusted_dex_volumes

    -- Fee Data
    , fundamental_data.fees_native as fees_native
    , fundamental_data.fees as chain_fees
    , fundamental_data.fees as fees
    , fundamental_data.fees / fundamental_data.txns as chain_avg_txn_fee
    , fundamental_data.median_txn_fee as chain_median_txn_fee
    , fundamental_data.l1_data_cost_native as l1_fee_allocation_native
    , fundamental_data.l1_data_cost as l1_fee_allocation
    , fundamental_data.fees - fundamental_data.l1_data_cost as equity_fee_allocation
    
    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem
    , contract_data.weekly_contracts_deployed
    , contract_data.weekly_contract_deployers

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
    
from fundamental_data
left join github_data using (date)
left join contract_data using (date)
left join defillama_data using (date)
left join rolling_metrics using (date)
left join scroll_dex_volumes using (date)
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
