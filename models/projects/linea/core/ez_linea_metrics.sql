{{
    config(
        materialized="incremental",
        snowflake_warehouse="LINEA",
        database="linea",
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
            , coalesce(daa, 0) as dau
            , coalesce(gas_usd, 0) as fees
            , coalesce(gas, 0) as fees_native
            , coalesce(revenue, 0) as revenue
            , coalesce(revenue_native, 0) as revenue_native
            , coalesce(l1_data_cost, 0) as l1_data_cost
            , coalesce(l1_data_cost_native, 0) as l1_data_cost_native
        from {{ ref("fact_linea_txns") }}
        left join {{ ref("fact_linea_daa") }} using (date)
        left join {{ ref("fact_linea_gas_gas_usd_revenue") }} using (date)
    )
    , github_data as ({{ get_github_metrics("linea") }})
    , contract_data as ({{ get_contract_metrics("linea") }})
    , defillama_data as ({{ get_defillama_metrics("linea") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("linea") }})
    , linea_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_linea_daily_dex_volumes") }}
    )
    
select 
   fundamental_data.date
    , 'linea' as artemis_id

    -- Standardized Metrics
    
    -- Usage Data
    , fundamental_data.dau as chain_dau
    , fundamental_data.dau as dau
    , wau as chain_wau
    , mau as chain_mau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns
    , fundamental_data.fees / fundamental_data.txns as chain_avg_txn_fee
    , defillama_data.tvl as chain_tvl
    , defillama_data.tvl as tvl
    , dune_dex_volumes_linea.dex_volumes as chain_spot_volume
    , dune_dex_volumes_linea.adjusted_dex_volumes
    
    -- Cashflow Metrics
    , fundamental_data.fees_native as fees_native
    , fundamental_data.fees as fees
    , fundamental_data.revenue as treasury_fee_allocation
    , fundamental_data.l1_data_cost as l1_fee_allocation
    
    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem
    , contract_data.weekly_contracts_deployed
    , contract_data.weekly_contract_deployers

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
    
from fundamental_data
left join github_data using (date)
left join contract_data using (date)
left join defillama_data using (date)
left join rolling_metrics using (date)
left join linea_dex_volumes as dune_dex_volumes_linea on fundamental_data.date = dune_dex_volumes_linea.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())