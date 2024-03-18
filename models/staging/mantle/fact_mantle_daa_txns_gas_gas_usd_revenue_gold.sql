{{ config(materialized="table") }}
select date, chain, daa, txns, gas, gas_usd, revenue
from {{ ref("fact_mantle_daa_txns_gas_gas_usd_revenue") }}
