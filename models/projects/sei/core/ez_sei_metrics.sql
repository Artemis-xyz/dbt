{{
    config(
        materialized="table",
        snowflake_warehouse="sei",
        database="sei",
        schema="core",
        alias="ez_metrics",
    )
}}
with
    sei_fundamental_metrics as (select * from {{ ref("fact_sei_daa_txns_gas_gas_usd_revenue") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("sei_wasm") }})
    , sei_avg_block_time as (select * from {{ ref("fact_sei_avg_block_time_silver") }})
    , price_data as ({{ get_coingecko_metrics("sei-network") }})
    , defillama_data as ({{ get_defillama_metrics("sei") }})
    , sei_evm_fundamental_metrics as (select * from {{ref("fact_sei_evm_fundamental_metrics_silver")}})
    , sei_evm_avg_block_time as (select * from {{ ref("fact_sei_evm_avg_block_time_silver") }})
    , rolling_evm_metrics as ({{ get_rolling_active_address_metrics("sei_evm") }})
select
    coalesce(wasm.date, evm.date, sei_avg_block_time.date, price.date, defillama.date) as date
    , (wasm_avg_block_time + evm_avg_block_time) / 2 as avg_block_time
    , (wasm_txns + evm_txns) / 86400 as avg_tps
    , 'sei' as chain
    , wasm_txns + coalesce(evm_txns, 0) as txns
    , wasm_daa + coalesce(evm_daa, 0) as dau
    , wasm_rolling_metrics.mau + evm_rolling_metrics.mau as mau
    , wasm_rolling_metrics.wau + evm_rolling_metrics.wau as wau
    , wasm_gas + coalesce(evm_gas, 0) as fees_native
    , wasm_gas_usd + coalesce(evm_gas_usd, 0) as fees
    , 0 as revenue
    , wasm_txns
    , wasm_daa as wasm_dau
    , wasm_rolling_metrics.mau as wasm_mau
    , wasm_rolling_metrics.wau as wasm_wau
    , wasm_gas as wasm_fees_native
    , wasm_gas_usd as wasm_fees
    , wasm_avg_block_time as wasm_avg_block_time
    , wasm_avg_tps as wasm_avg_tps
    , 0 as wasm_revenue
    , tvl
    , dex_volumes
    , price
    , market_cap
    , evm_txns
    , evm_daa as evm_dau
    , evm_rolling_metrics.mau as evm_mau
    , evm_rolling_metrics.wau as evm_wau
    , evm_avg_tps
    , evm_gas as evm_fees_native
    , evm_gas_usd as evm_fees
    , 0 as evm_revenue
    , evm_avg_block_time 
from sei_fundamental_metrics as wasm
full join rolling_metrics as wasm_rolling_metrics using (date)
full join sei_avg_block_time as sei_avg_block_time using (date)
full join sei_evm_avg_block_time using (date)
full join price_data as price using (date)
full join defillama_data as defillama using (date)
full join sei_evm_fundamental_metrics as evm using (date)
full join rolling_evm_metrics as evm_rolling_metrics using (date)
where 
coalesce(wasm.date, evm.date, sei_avg_block_time.date, price.date, defillama.date) < date(sysdate())
