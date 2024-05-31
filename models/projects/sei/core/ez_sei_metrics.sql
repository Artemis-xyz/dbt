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
    , sei_avg_block_time as (select * from {{ ref("fact_sei_avg_block_time_silver") }})
    , price_data as ({{ get_coingecko_metrics("sei-network") }})
    , defillama_data as ({{ get_defillama_metrics("sei") }})
    , sei_evm_fundamental_metrics as (select * from {{ref("fact_sei_evm_fundamental_metrics_silver")}})
    , sei_evm_avg_block_time as (select * from {{ ref("fact_sei_evm_avg_block_time_silver") }})
select
    coalesce(wasm.date, evm.date, sei_avg_block_time.date, price.date, defillama.date) as date
    , avg_block_time
    , avg_tps
    , 'sei' as chain
    , txns + coalesce(evm_txns, 0) as txns
    , daa + coalesce(evm_daa, 0) as dau
    , gas + coalesce(evm_gas, 0) as fees_native
    , gas_usd + coalesce(evm_gas_usd, 0) as fees
    , 0 as revenue
    , txns as wasm_txns
    , daa as wasm_dau
    , gas as wasm_fees_native
    , gas_usd as wasm_fees
    , avg_block_time as wasm_avg_block_time
    , avg_tps as wasm_avg_tps
    , 0 as wasm_revenue
    , tvl
    , dex_volumes
    , price
    , market_cap
    , evm_txns
    , evm_daa as evm_dau
    , evm_avg_tps
    , evm_gas as evm_fees_native
    , evm_gas_usd as evm_fees
    , 0 as evm_revenue
    , evm_avg_block_time 
from sei_fundamental_metrics as wasm
full join sei_avg_block_time as sei_avg_block_time using (date)
full join sei_evm_avg_block_time using (date)
full join price_data as price using (date)
full join defillama_data as defillama using (date)
full join sei_evm_fundamental_metrics as evm using (date)
where 
coalesce(wasm.date, evm.date, sei_avg_block_time.date, price.date, defillama.date) < date(sysdate())
