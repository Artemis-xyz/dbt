{{ config(materialized="table") }}
select date, chain, daa, txns, gas, gas_usd
from {{ ref("fact_parallel_finance_daa_gas_gas_usd_txns") }}
