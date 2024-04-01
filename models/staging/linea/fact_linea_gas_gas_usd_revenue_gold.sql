{{ config(materialized="table") }}
select date, chain, gas, gas_usd, revenue
from {{ ref("fact_linea_gas_gas_usd_revenue") }}
