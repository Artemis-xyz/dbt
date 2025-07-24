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
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
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
    , price_data as ({{ get_coingecko_metrics("scroll") }})
select
    fd.date
    , 'scroll' as chain
    , fd.txns
    , fd.dau
    , rm.wau
    , rm.mau
    , fd.fees
    , fd.fees / fd.txns as avg_txn_fee
    , fd.median_txn_fee
    , fd.fees_native
    , fd.revenue
    , fd.revenue_native
    , dsv.dex_volumes
    , dsv.adjusted_dex_volumes
    , fd.l1_data_cost
    , fd.l1_data_cost_native

    -- Standardized Metrics
    -- Market Data
    , pd.price
    , pd.market_cap
    , pd.fdmc
    , pd.token_volume
    , pd.token_turnover_circulating

    -- Chain Metrics
    , fd.dau as chain_dau
    , rm.wau as chain_wau
    , rm.mau as chain_mau
    , fd.txns as chain_txns
    , dsv.dex_volumes as chain_spot_volume

    -- Cashflow Metrics
    , fd.fees as chain_fees
    , fd.fees as ecosystem_revenue
    , fd.fees_native as ecosystem_revenue_native
    , avg_txn_fee as chain_avg_txn_fee
    , fd.median_txn_fee as chain_median_txn_fee
    , fd.l1_data_cost as l1_fee_allocation
    , fd.l1_data_cost_native as l1_fee_allocation_native
    , coalesce(fd.fees, 0) - coalesce(fd.l1_data_cost, 0) as equity_fee_allocation
    
    -- Developer Metrics
    , gd.weekly_commits_core_ecosystem
    , gd.weekly_commits_sub_ecosystem
    , gd.weekly_developers_core_ecosystem
    , gd.weekly_developers_sub_ecosystem
    , cd.weekly_contracts_deployed
    , cd.weekly_contract_deployers

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data fd
left join github_data gd on fd.date = gd.date
left join contract_data cd on fd.date = cd.date
left join defillama_data dd on fd.date = dd.date
left join rolling_metrics rm on fd.date = rm.date
left join scroll_dex_volumes dsv on fd.date = dsv.date
left join price_data pd on fd.date = pd.date
where true
{{ ez_metrics_incremental('fd.date', backfill_date) }}
and fd.date < to_date(sysdate())
