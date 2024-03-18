{{ config(materialized="table") }}
select date, chain, gas, gas_usd, fees, revenue
from {{ ref("fact_fantom_gas_gas_usd_fees_revenue") }}
