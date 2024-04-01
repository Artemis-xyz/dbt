{{ config(materialized="table") }}
select date, gas, gas_usd, revenue, txns, daa, chain
from {{ ref("fact_sei_daa_txns_gas_gas_usd_revenue") }}
