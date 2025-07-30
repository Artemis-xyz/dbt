-- depends_on {{ ref("fact_sei_transactions_v2") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="SEI",
        database="sei",
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
    sei_combined_fundamental_metrics as ( {{ get_fundamental_data_for_chain("sei", "v2") }} )
    , sei_fundamental_metrics as (
        select * 
        from {{ ref("fact_sei_daa_txns_gas_gas_usd_revenue") }}
    )
    , rolling_metrics as ({{ get_rolling_active_address_metrics("sei") }})
    , contract_data as ({{ get_contract_metrics("sei") }})
    , sei_avg_block_time as (
        select * 
        from {{ ref("fact_sei_avg_block_time_silver") }}
    )
    , market_metrics as ({{ get_coingecko_metrics("sei-network") }})
    , defillama_data as ({{ get_defillama_metrics("sei") }})
    , sei_evm_fundamental_metrics as (
        select * 
        from {{ref("fact_sei_evm_fundamental_metrics_silver")}}
    )
    , sei_evm_avg_block_time as (
        select * 
        from {{ ref("fact_sei_evm_avg_block_time_silver") }}
    )
    , sei_emissions as (
        select date, coalesce(rewards_amount, 0) as mints_native 
        from {{ ref("fact_sei_emissions") }}
    )
    , sei_dex_volumes as (
        select date, coalesce(daily_volume, 0) as dex_volumes, coalesce(daily_volume_adjusted, 0) as adjusted_dex_volumes
        from {{ ref("fact_sei_daily_dex_volumes") }}
    )
    , sei_supply as (
        select date, premine_unlocks_native, net_supply_change_native, burns_native, circulating_supply_native 
        from {{ ref("fact_sei_supply_data") }}
    )
select
    coalesce(sei_combined_fundamental_metrics.date, sei_fundamental_metrics.date, sei_evm_fundamental_metrics.date, sei_avg_block_time.date, market_metrics.date, defillama_data.date, contract_data.date) as date
    , 'sei' as artemis_id
    , 'sei' as chain
    
    -- Standardized Metrics
    
    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , sei_combined_fundamental_metrics.dau as chain_dau
    , sei_combined_fundamental_metrics.dau as dau
    , sei_combined_fundamental_metrics.txns as chain_txns
    , sei_combined_fundamental_metrics.txns as txns
    , rolling_metrics.mau as chain_mau
    , rolling_metrics.wau as chain_wau
    , (wasm_avg_block_time + evm_avg_block_time) / 2 as chain_avg_block_time
    , (wasm_txns + evm_txns) / 86400 as chain_avg_tps
    , sei_dex_volumes.dex_volumes AS chain_spot_volume
    , sei_dex_volumes.adjusted_dex_volumes as adjusted_dex_volumes

    -- Fee Data
    , sei_combined_fundamental_metrics.fees_native as fees_native
    , sei_combined_fundamental_metrics.fees as chain_fees
    , sei_combined_fundamental_metrics.fees as fees
    , sei_combined_fundamental_metrics.avg_txn_fee as avg_txn_fee
    , sei_combined_fundamental_metrics.l1_data_cost_native as l1_fee_allocation_native
    , sei_combined_fundamental_metrics.l1_data_cost as l1_fee_allocation
    , sei_combined_fundamental_metrics.fees - sei_combined_fundamental_metrics.l1_data_cost as equity_fee_allocation

    -- Financial Statements
    , 0 as revenue_native
    , 0 as revenue
    
    -- Supply Metrics
    , sei_emissions.mints_native as gross_emissions_native
    , sei_supply.premine_unlocks_native
    , sei_supply.burns_native
    , sei_supply.circulating_supply_native + coalesce(sei_emissions.mints_native, 0) as circulating_supply_native

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- Developer Metrics
    , weekly_contracts_deployed
    , weekly_contract_deployers

    -- Bespoke Metrics
    , sei_fundamental_metrics.wasm_txns
    , sei_fundamental_metrics.wasm_daa as wasm_dau
    , sei_fundamental_metrics.wasm_returning_users
    , sei_fundamental_metrics.wasm_new_users
    , sei_fundamental_metrics.wasm_gas as wasm_fees_native
    , sei_fundamental_metrics.wasm_gas_usd as wasm_fees
    , sei_fundamental_metrics.wasm_avg_block_time as wasm_avg_block_time
    , sei_fundamental_metrics.wasm_avg_tps as wasm_avg_tps
    , sei_evm_fundamental_metrics.evm_avg_block_time 
    , sei_evm_fundamental_metrics.evm_new_users
    , sei_evm_fundamental_metrics.evm_returning_users
    , sei_evm_fundamental_metrics.evm_txns
    , sei_evm_fundamental_metrics.evm_daa as evm_dau
    , sei_evm_fundamental_metrics.evm_avg_tps
    , sei_evm_fundamental_metrics.evm_gas as evm_fees_native
    , sei_evm_fundamental_metrics.evm_gas_usd as evm_fees
    , sei_evm_fundamental_metrics.tvl

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from sei_combined_fundamental_metrics
full join contract_data using (date)
full join sei_fundamental_metrics using (date)
full join sei_evm_fundamental_metrics using (date)
full join rolling_metrics using (date)
full join sei_avg_block_time using (date)
full join sei_evm_avg_block_time using (date)
full join market_metrics using (date)
full join defillama_data using (date)
full join sei_emissions using (date)
left join sei_dex_volumes using (date)
full join sei_supply using (date)
where true
{{ ez_metrics_incremental('coalesce(sei_combined_fundamental_metrics.date, sei_fundamental_metrics.date, sei_evm_fundamental_metrics.date, sei_avg_block_time.date, market_metrics.date, defillama_data.date, contract_data.date)', backfill_date) }}
and coalesce(sei_combined_fundamental_metrics.date, sei_fundamental_metrics.date, sei_evm_fundamental_metrics.date, sei_avg_block_time.date, market_metrics.date, defillama_data.date, contract_data.date) < date(sysdate())
