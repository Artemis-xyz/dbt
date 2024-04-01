{{ config(materialized="table") }}
select date, daa, txns, gas, gas_usd, revenue, chain
from {{ ref("fact_aptos_daa_txns_gas_gas_usd_revenue") }}
