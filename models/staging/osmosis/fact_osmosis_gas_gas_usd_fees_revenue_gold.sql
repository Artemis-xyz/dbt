{{ config(materialized="table") }}
select date, chain, gas, gas_usd, trading_fees, fees, revenue
from {{ ref("fact_osmosis_gas_gas_usd_fees_revenue") }}
