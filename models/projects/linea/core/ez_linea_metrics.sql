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
            , revenue
            , revenue_native
            , l1_data_cost
            , l1_data_cost_native
        from {{ ref("fact_linea_txns") }}
        left join {{ ref("fact_linea_daa") }} using (date)
        left join {{ ref("fact_linea_gas_gas_usd_revenue") }} using (date)
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , github_data as ({{ get_github_metrics("linea") }})
    , contract_data as ({{ get_contract_metrics("linea") }})
    , defillama_data as ({{ get_defillama_metrics("linea") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("linea") }})
    , linea_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_linea_daily_dex_volumes") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    
select 
   fundamental_data.date
    , 'linea' as chain
    , txns
    , dau
    , wau
    , mau
    , fees
    , fees_native
    , fees / txns as avg_txn_fee
    , revenue
    , revenue_native
    , l1_data_cost
    , l1_data_cost_native
    , dune_dex_volumes_linea.dex_volumes
    , dune_dex_volumes_linea.adjusted_dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , tvl
    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , dune_dex_volumes_linea.dex_volumes AS chain_spot_volume
    , fees / txns as chain_avg_txn_fee
    -- Cashflow Metrics
    , fees AS ecosystem_revenue
    , fees_native AS ecosystem_revenue_native
    , revenue AS treasury_fee_allocation
    , revenue_native AS treasury_fee_allocation_native
    , l1_data_cost_native AS l1_fee_allocation_native
    , l1_data_cost AS l1_fee_allocation
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join github_data using (date)
left join contract_data using (date)
left join defillama_data using (date)
left join rolling_metrics using (date)
left join linea_dex_volumes as dune_dex_volumes_linea on fundamental_data.date = dune_dex_volumes_linea.date
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())