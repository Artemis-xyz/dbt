{{
    config(
        materialized="incremental"
        , snowflake_warehouse="ZORA"
        , database="zora"
        , schema="core"
        , alias="ez_metrics"
        , incremental_strategy="merge"
        , unique_key="date"
        , on_schema_change="append_new_columns"
        , merge_update_columns=var("backfill_columns", [])
        , merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none
        , full_refresh=false
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
            , median_gas as median_txn_fee
            , revenue
            , revenue_native
            , l1_data_cost
            , l1_data_cost_native
        from {{ ref("fact_zora_txns") }}
        left join {{ ref("fact_zora_daa") }} using (date)
        left join {{ ref("fact_zora_gas_gas_usd_revenue") }} using (date)
    )
    , github_data as ({{ get_github_metrics("zora") }})
    , contract_data as ({{ get_contract_metrics("zora") }})
    , defillama_data as ({{ get_defillama_metrics("zora") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("zora") }})
    , zora_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_zora_daily_dex_volumes") }}
    )
select
    fundamental_data.date
    , dune_dex_volumes_zora.dex_volumes
    , dune_dex_volumes_zora.adjusted_dex_volumes
    , 'zora' as chain
    , txns
    , dau
    , wau
    , mau
    , fees
    , fees_native
    , median_txn_fee
    , fees / txns as avg_txn_fee
    , revenue
    , revenue_native
    , l1_data_cost
    , l1_data_cost_native
    -- Standardized Metrics
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    , wau as chain_wau
    , mau as chain_mau
    , median_txn_fee as chain_median_txn_fee
    , avg_txn_fee as chain_avg_txn_fee
    -- Cash Flow Metrics
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    , l1_data_cost as l1_fee_allocation
    , l1_data_cost_native as l1_fee_allocation_native
    , revenue as treasury_fee_allocation
    , revenue_native as treasury_fee_allocation_native
    -- Crypto Metrics
    , tvl
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from fundamental_data
left join github_data using (date)
left join contract_data using (date)
left join defillama_data using (date)
left join rolling_metrics using (date)
left join zora_dex_volumes as dune_dex_volumes_zora on fundamental_data.date = dune_dex_volumes_zora.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())