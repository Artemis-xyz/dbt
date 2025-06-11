-- depends_on {{ ref("fact_sei_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="SEI",
        database="sei",
        schema="core",
        alias="ez_metrics",
    )
}}
with
    sei_combined_fundamental_metrics as ( {{ get_fundamental_data_for_chain("sei", "v2") }} )
    , sei_fundamental_metrics as (select * from {{ ref("fact_sei_daa_txns_gas_gas_usd_revenue") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("sei") }})
    , contract_data as ({{ get_contract_metrics("sei") }})
    , sei_avg_block_time as (select * from {{ ref("fact_sei_avg_block_time_silver") }})
    , price_data as ({{ get_coingecko_metrics("sei-network") }})
    , defillama_data as ({{ get_defillama_metrics("sei") }})
    , sei_evm_fundamental_metrics as (select * from {{ref("fact_sei_evm_fundamental_metrics_silver")}})
    , sei_evm_avg_block_time as (select * from {{ ref("fact_sei_evm_avg_block_time_silver") }})
    , sei_emissions as (select date, rewards_amount as mints_native from {{ ref("fact_sei_emissions") }})
    , sei_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_sei_daily_dex_volumes") }}
    )
    , sei_supply as (
        select date, premine_unlocks_native, net_supply_change_native, burns_native, circulating_supply_native 
        from {{ ref("fact_sei_supply_data") }}
    )
select
    coalesce(combined.date, wasm.date, evm.date, sei_avg_block_time.date, price.date, defillama.date, contracts.date) as date
    , 'sei' as chain
    , (wasm_avg_block_time + evm_avg_block_time) / 2 as avg_block_time
    , (wasm_txns + evm_txns) / 86400 as avg_tps
    , combined.txns as txns
    , combined.dau as dau
    , rolling_metrics.mau as mau
    , rolling_metrics.wau as wau
    , combined.fees_native as fees_native
    , combined.fees as fees
    , combined.avg_txn_fee as avg_txn_fee
    , 0 as revenue_native
    , 0 as revenue
    , 0 as evm_revenue
    , 0 as wasm_revenue
    , dune_dex_volumes_sei.dex_volumes AS dex_volumes
    , dune_dex_volumes_sei.adjusted_dex_volumes AS adjusted_dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , combined.txns as chain_txns
    , combined.dau as chain_dau
    , rolling_metrics.mau as chain_mau
    , rolling_metrics.wau as chain_wau
    , combined.new_users as new_users
    , combined.returning_users as returning_users
    , combined.avg_txn_fee as chain_avg_txn_fee
    , (wasm_avg_block_time + evm_avg_block_time) / 2 as chain_avg_block_time
    , (wasm_txns + evm_txns) / 86400 as chain_avg_tps
    , dune_dex_volumes_sei.dex_volumes AS chain_spot_volume
    -- EVM Usage Metrics
    , evm_avg_block_time 
    , evm_new_users
    , evm_returning_users
    , evm_txns
    , evm_daa as evm_dau
    , evm_avg_tps
    , evm_gas as evm_fees_native
    , evm_gas_usd as evm_fees
    -- Wasm Usage Metrics
    , wasm_txns
    , wasm_daa as wasm_dau
    , wasm_returning_users
    , wasm_new_users
    , wasm_gas as wasm_fees_native
    , wasm_gas_usd as wasm_fees
    , wasm_avg_block_time as wasm_avg_block_time
    , wasm_avg_tps as wasm_avg_tps
    -- Cashflow Metrics
    , combined.fees as chain_fees
    , combined.fees_native as ecosystem_revenue_native
    , combined.fees as ecosystem_revenue
    , 0 as evm_fee_allocation_native
    , 0 as wasm_fee_allocation_native
    -- Supply Metrics
    , sei_emissions.mints_native as gross_emissions_native
    , sei_supply.premine_unlocks_native
    , sei_supply.net_supply_change_native
    , sei_supply.burns_native
    , sei_supply.circulating_supply_native + coalesce(sei_emissions.mints_native, 0) as circulating_supply_native
    -- Developer Metrics
    , weekly_contracts_deployed
    , weekly_contract_deployers
from sei_combined_fundamental_metrics as combined
full join contract_data as contracts using (date)
full join sei_fundamental_metrics as wasm using (date)
full join sei_evm_fundamental_metrics as evm using (date)
full join rolling_metrics using (date)
full join sei_avg_block_time as sei_avg_block_time using (date)
full join sei_evm_avg_block_time using (date)
full join price_data as price using (date)
full join defillama_data as defillama using (date)
full join sei_emissions using (date)
left join sei_dex_volumes as dune_dex_volumes_sei using (date)
full join sei_supply as sei_supply using (date)
where 
coalesce(combined.date, wasm.date, evm.date, sei_avg_block_time.date, price.date, defillama.date, contracts.date) < date(sysdate())
