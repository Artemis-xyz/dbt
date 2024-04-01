{{ config(materialized="table") }}
select date, chain, gas, gas_usd, txns
from {{ ref("fact_zcash_gas_gas_usd_txns") }}
