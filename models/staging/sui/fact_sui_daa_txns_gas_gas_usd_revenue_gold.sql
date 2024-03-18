{{ config(materialized="table") }}
select date, daa, txns, gas, gas_usd, revenue, chain
from {{ ref("fact_sui_daa_txns_gas_gas_usd_revenue") }}
