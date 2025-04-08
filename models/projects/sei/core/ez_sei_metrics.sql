-- depends_on {{ ref("ez_sei_transactions_v2") }}
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
        select date, daily_volume as dex_volumes
        from {{ ref("fact_sei_daily_dex_volumes") }}
    )
select
    coalesce(combined.date, wasm.date, evm.date, sei_avg_block_time.date, price.date, defillama.date, contracts.date) as date
    , (wasm_avg_block_time + evm_avg_block_time) / 2 as avg_block_time
    , (wasm_txns + evm_txns) / 86400 as avg_tps
    , 'sei' as chain
    , combined.txns as txns
    , combined.dau as dau
    , combined.new_users as new_users
    , combined.returning_users as returning_users
    , rolling_metrics.mau as mau
    , rolling_metrics.wau as wau
    , combined.fees_native as fees_native
    , combined.fees as fees
    , combined.avg_txn_fee as avg_txn_fee
    , 0 as revenue_native
    , 0 as revenue
    , wasm_txns
    , wasm_daa as wasm_dau
    , wasm_returning_users
    , wasm_new_users
    , wasm_gas as wasm_fees_native
    , wasm_gas_usd as wasm_fees
    , wasm_avg_block_time as wasm_avg_block_time
    , wasm_avg_tps as wasm_avg_tps
    , 0 as wasm_revenue
    , tvl
    , price
    , market_cap
    , evm_new_users
    , evm_returning_users
    , evm_txns
    , evm_daa as evm_dau
    , evm_avg_tps
    , evm_gas as evm_fees_native
    , evm_gas_usd as evm_fees
    , 0 as evm_revenue
    , evm_avg_block_time 
    , sei_emissions.mints_native
    , weekly_contracts_deployed
    , weekly_contract_deployers
    , dune_dex_volumes_sei.dex_volumes
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
where 
coalesce(combined.date, wasm.date, evm.date, sei_avg_block_time.date, price.date, defillama.date, contracts.date) < date(sysdate())
